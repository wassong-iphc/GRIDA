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
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class PoolUpload extends Thread {

    private static final Logger logger = Logger.getLogger(PoolUpload.class);
    private static PoolUpload instance;
    private PoolDAO poolDAO;
    private static volatile int running = 0;

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

                    if (running < Configuration.getInstance().getMaxSimultaneousUploads()) {
                        running++;
                        logger.info("[Pool Upload] Processing operation '" + operation.getId() + "'");
                        updateStatus(operation, Status.Running);
                        new Upload(operation).start();

                    } else {
                        break;
                    }
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

        public Upload(Operation operation) {

            this.operation = operation;
        }

        @Override
        public void run() {

            try {
                OperationBusiness operationBusiness = new OperationBusiness(operation.getProxy());

                String path = operationBusiness.uploadFile(operation.getSource(), operation.getDest());
                try {
                    new PoolBusiness().addOperation(operation.getProxy(), path, "",
                            Operation.Type.Replicate.name(), operation.getUser());

                } catch (BusinessException ex) {
                }
                updateStatus(operation, Status.Done);

            } catch (DAOException ex) {
                retry();
            } catch (BusinessException ex) {
                logger.error(ex);
                retry();
            } finally {
                running--;
                PoolUpload.getInstance();
            }
        }

        private void retry() {

            try {
                if (operation.getRetrycount() == Configuration.getInstance().getMaxRetryCount()) {
                    updateStatus(operation, Status.Failed);
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
