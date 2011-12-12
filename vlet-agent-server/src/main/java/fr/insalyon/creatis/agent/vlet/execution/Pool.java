/* Copyright CNRS-CREATIS
 *
 * Rafael Silva
 * rafael.silva@creatis.insa-lyon.fr
 * http://www.rafaelsilva.com
 *
 * This software is a grid-enabled data-driven workflow manager and editor.
 *
 * This software is governed by the CeCILL  license under French law and
 * abiding by the rules of distribution of free software.  You can  use,
 * modify and/ or redistribute the software under the terms of the CeCILL
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and  rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty  and the software's author,  the holder of the
 * economic rights,  and the successive licensors  have only  limited
 * liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading,  using,  modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean  that it is complicated to manipulate,  and  that  also
 * therefore means  that it is reserved for developers  and  experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systems and/or
 * data to be ensured and,  more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL license and that you accept its terms.
 */
package fr.insalyon.creatis.agent.vlet.execution;

import fr.insalyon.creatis.agent.vlet.execution.operation.Operations;
import fr.insalyon.creatis.agent.vlet.Configuration;
import fr.insalyon.creatis.agent.vlet.common.Constants;
import fr.insalyon.creatis.agent.vlet.common.bean.Operation;
import fr.insalyon.creatis.agent.vlet.common.bean.Operation.Status;
import fr.insalyon.creatis.agent.vlet.dao.DAOException;
import fr.insalyon.creatis.agent.vlet.dao.DAOFactory;
import fr.insalyon.creatis.agent.vlet.dao.PoolDAO;
import fr.insalyon.creatis.agent.vlet.execution.operation.LCGOperations;
import fr.insalyon.creatis.agent.vlet.execution.operation.VletOperations;
import java.io.File;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class Pool extends Thread {

    private static final Logger logger = Logger.getLogger(Pool.class);
    private static Pool instance;
    private PoolDAO poolDAO;

    public synchronized static Pool getInstance() {
        if (instance == null) {
            instance = new Pool();
            instance.start();
        }
        return instance;
    }

    private Pool() {
    }

    @Override
    public void run() {

        poolDAO = DAOFactory.getDAOFactory().getPoolDAO();
        boolean stop = false;

        while (!stop) {

            try {
                List<Operation> pendingOperations = poolDAO.getPendingOperations();
                processPendingOperations(pendingOperations);

            } catch (DAOException ex) {
                logger.error(ex);
            }
            stop = true;

            try {
                processReplicateOperations();
                stop = false;

            } catch (DAOException ex) {
                if (!ex.getMessage().contains("No data is available")) {
                    logger.error(ex);
                }
            }

            try {
                processDeleteOperations();
                stop = false;

            } catch (DAOException ex) {
                if (!ex.getMessage().contains("No data is available")) {
                    logger.error(ex);
                }
            }
        }
        instance = null;
    }

    private void processReplicateOperations() throws DAOException {
        Operation operation = poolDAO.getReplicateOperation();
        try {
            updateStatus(operation, Status.Running);
            Operations.replicateFile(operation.getProxy(), operation.getSource());
            poolDAO.removeOperationById(operation.getId());

        } catch (Exception ex) {
            processOperationException(ex, operation);
        }
    }

    private void processDeleteOperations() throws DAOException {
        Operation operation = poolDAO.getDeleteOperation();
        try {
            updateStatus(operation, Status.Running);
            Operations.delete(operation.getProxy(), operation.getSource());
            poolDAO.removeOperationById(operation.getId());

        } catch (Exception ex) {
            processOperationException(ex, operation);
        }
    }

    private void processOperationException(Exception ex, Operation operation) throws DAOException {

        logger.error(ex.getMessage());
        if (operation.getRetrycount() == Configuration.getInstance().getMaxRetryCount()) {
            poolDAO.removeOperationById(operation.getId());
        } else {
            operation.incrementRetryCount();
            updateStatus(operation, Status.Rescheduled);
        }
    }

    private void processPendingOperations(List<Operation> pendingOperations) throws DAOException {

        while (!pendingOperations.isEmpty()) {
            for (Operation operation : pendingOperations) {

                logger.info("Processing operation '" + operation.getId() + "'");
                try {
                    updateStatus(operation, Status.Running);

                    if (operation.getType() == Operation.Type.Upload) {

                        String path = Operations.uploadFile(operation.getProxy(),
                                operation.getSource(), operation.getDest());
                        try {
                            addOperation(operation.getProxy(), path, "",
                                    Operation.Type.Replicate.name(),
                                    operation.getUser());

                        } catch (Exception ex) {
                            logger.warn(ex);
                        }

                    } else if (operation.getType() == Operation.Type.Download) {

                        boolean isDir = false;
                        if (Configuration.getInstance().useLcgCommands()) {
                            isDir = LCGOperations.isDir(operation.getProxy(), operation.getSource());
                        } else {
                            isDir = VletOperations.isDir(operation.getProxy(), operation.getSource());
                        }
                        if (isDir) {
                            String zipFile = Operations.downloadFolder(
                                    operation.getProxy(), operation.getDest(),
                                    operation.getSource());
                            operation.setDest(zipFile);

                        } else {
                            String fileName = FilenameUtils.getName(operation.getSource());
                            Operations.downloadFile(operation.getProxy(),
                                    operation.getDest(), fileName, operation.getSource());
                        }

                    } else if (operation.getType() == Operation.Type.Download_Files) {

                        File file = new File(operation.getDest());
                        String zipFile = Operations.downloadFiles(
                                operation.getProxy(), operation.getDest(),
                                operation.getSource().split(Constants.LEVEL_SEPARATOR),
                                file.getName());
                        operation.setDest(zipFile);
                    }
                    updateStatus(operation, Status.Done);

                } catch (Exception ex) {
                    if (operation.getRetrycount() == Configuration.getInstance().getMaxRetryCount()) {
                        updateStatus(operation, Status.Failed);
                    } else {
                        operation.incrementRetryCount();
                        updateStatus(operation, Status.Rescheduled);
                    }
                    logger.error(ex);
                }
            }
            pendingOperations = poolDAO.getPendingOperations();
        }
    }

    private void updateStatus(Operation operation, Status status) throws DAOException {
        operation.setStatus(status);
        poolDAO.updateOperation(operation);
    }

    public void addOperation(String proxyFileName, String source, String dest,
            String operation, String user) throws DAOException {

        String id = operation + "-" + System.nanoTime();
        Operation op = new Operation(id, source, dest, operation, user, proxyFileName);
        poolDAO.addOperation(op);
    }
}
