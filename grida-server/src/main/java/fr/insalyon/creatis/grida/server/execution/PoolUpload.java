/* Copyright CNRS-CREATIS
 *
 * Rafael Ferreira da Silva
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
package fr.insalyon.creatis.grida.server.execution;

import fr.insalyon.creatis.grida.common.bean.Operation;
import fr.insalyon.creatis.grida.common.bean.Operation.Status;
import fr.insalyon.creatis.grida.server.Configuration;
import fr.insalyon.creatis.grida.server.business.BusinessException;
import fr.insalyon.creatis.grida.server.business.OperationBusiness;
import fr.insalyon.creatis.grida.server.business.PoolBusiness;
import fr.insalyon.creatis.grida.server.dao.DAOException;
import fr.insalyon.creatis.grida.server.dao.DAOFactory;
import fr.insalyon.creatis.grida.server.dao.PoolDAO;
import java.io.File;
import java.util.List;
import java.util.concurrent.Semaphore;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Ferreira da Silva
 */
public class PoolUpload extends Thread {

    private static final Logger logger = Logger.getLogger(PoolUpload.class);
    private static PoolUpload instance;
    private PoolDAO poolDAO;
    private static Semaphore counter = new Semaphore(
        Configuration.getInstance().getMaxSimultaneousUploads());

    public synchronized static PoolUpload getInstance() {
        if (instance == null) {
            instance = new PoolUpload();
            instance.start();
        }
        return instance;
    }

    private PoolUpload() {
    }

    @Override
    public void run() {

        try {
            poolDAO = DAOFactory.getDAOFactory().getPoolDAO();
            List<Operation> pendingOperations = poolDAO.getUploadPendingOperations();

            while (!pendingOperations.isEmpty()) {

                for (Operation operation : pendingOperations) {
                    counter.acquireUninterruptibly();
                    logger.info("[Pool Upload] Processing operation '" + operation.getId() + "'");
                    updateStatus(operation, Status.Running);
                    new Upload(operation, counter).start();
                }
                pendingOperations = poolDAO.getUploadPendingOperations();
            }
        } catch (DAOException ex) {
            //do nothing
        }
        instance = null;
    }

    private void updateStatus(Operation operation, Status status) throws DAOException {

        operation.setStatus(status);
        poolDAO.updateOperation(operation);
    }

    class Upload extends Thread {

        private Operation operation;
        private Semaphore counter;

        public Upload(Operation operation, Semaphore counter) {

            this.operation = operation;
            this.counter = counter;
        }

        @Override
        public void run() {

            try {
                OperationBusiness operationBusiness = new OperationBusiness(operation.getProxy());

                String path = operationBusiness.uploadFile(operation.getId(),
                        operation.getSource(), operation.getDest());
                try {
                    new PoolBusiness().addOperation(operation.getProxy(), path, "",
                            Operation.Type.Replicate, operation.getUser());

                } catch (BusinessException ex) {
                    // do nothing
                }
                updateStatus(operation, Status.Done);

            } catch (DAOException ex) {
                retry();
            } catch (BusinessException ex) {
                logger.error(ex);
                retry();
            } finally {
                counter.release();
            }
        }

        private void retry() {

            try {
                if (operation.getRetrycount() == Configuration.getInstance().getMaxRetryCount()) {
                    updateStatus(operation, Status.Failed);
                    FileUtils.deleteQuietly(new File(operation.getSource()));

                } else {
                    operation.incrementRetryCount();
                    updateStatus(operation, Status.Rescheduled);
                }
            } catch (DAOException ex) {
                // do nothing
            }
        }
    }
}
