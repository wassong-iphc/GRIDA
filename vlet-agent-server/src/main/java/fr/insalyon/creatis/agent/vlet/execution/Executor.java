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

import fr.insalyon.creatis.agent.vlet.Communication;
import fr.insalyon.creatis.agent.vlet.common.Constants;
import fr.insalyon.creatis.agent.vlet.common.ExecutorConstants;
import fr.insalyon.creatis.agent.vlet.execution.command.AllCachedFilesCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.Command;
import fr.insalyon.creatis.agent.vlet.execution.command.CreateDirectoryCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.DeleteCachedFileCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.DeleteCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.ExistDataCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.GetModificationDateCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.GetRemoteFileCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.GetRemoteFolderCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.ListFilesAndFoldersCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.RenameCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.pool.PoolAddOperationCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.pool.PoolOperationByIdCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.ReplicatePreferredSEsCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.UploadFileCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.pool.PoolAllOperationsCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.pool.PoolClearDeleteOperationsCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.pool.PoolOperationsByUserCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.pool.PoolRemoveOperationByIdCommand;
import fr.insalyon.creatis.agent.vlet.execution.command.pool.PoolRemoveOperationsByUserCommand;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class Executor extends Thread {

    private static final Logger logger = Logger.getLogger(Executor.class);
    private Communication communication;
    private boolean poolExecutor = true;

    public Executor(Communication communication) {
        this.communication = communication;
    }

    @Override
    public void run() {

        String message = communication.getMessage();
        if (message != null) {
            String[] tokens = message.split(Constants.SEPARATOR);
            Command command = parseCommand(tokens);

            if (command != null) {
                if (poolExecutor) {
                    ExecutorPool.getInstance().add(command);
                } else {
                    command.execute();
                }
            }
        } else {
            logException(new Exception("Error during message receive: " + message));
        }
    }

    private Command parseCommand(String[] tokens) {

        try {
            String command = tokens[0];
            String proxy = tokens[1];

            if (command.equals(ExecutorConstants.REG_GET_REMOTE_FILE)) {
                return new GetRemoteFileCommand(communication, proxy, tokens[2], tokens[3]);

            } else if (command.equals(ExecutorConstants.REG_GET_REMOTE_FOLDER)) {
                return new GetRemoteFolderCommand(communication, proxy, tokens[2], tokens[3]);

            } else if (command.equals(ExecutorConstants.REG_LIST_FILES_AND_FOLDERS)) {
                return new ListFilesAndFoldersCommand(communication, proxy, tokens[2], tokens[3]);

            } else if (command.equals(ExecutorConstants.REG_GET_MODIFICATION_DATE)) {
                return new GetModificationDateCommand(communication, proxy, tokens[2]);

            } else if (command.equals(ExecutorConstants.REG_UPLOAD_FILE)) {
                return new UploadFileCommand(communication, proxy, tokens[2], tokens[3]);

            } else if (command.equals(ExecutorConstants.REG_UPLOAD_FILE_TO_SES)) {
                return new UploadFileCommand(communication, proxy, tokens[2], tokens[3], tokens[4].split(Constants.INTRA_SEPARATOR));

            } else if (command.equals(ExecutorConstants.REG_REPLICATE_PREFERRED_SES)) {
                return new ReplicatePreferredSEsCommand(communication, proxy, tokens[2]);

            } else if (command.equals(ExecutorConstants.REG_DELETE)) {
                return new DeleteCommand(communication, proxy, tokens[2].split(Constants.INTRA_SEPARATOR));

            } else if (command.equals("DeleteFile")) {//TODO: remove it
                return new DeleteCommand(communication, proxy, tokens[2]);

            } else if (command.equals(ExecutorConstants.REG_CREATE_DIRECTORY)) {
                return new CreateDirectoryCommand(communication, proxy, tokens[2]);

            } else if (command.equals(ExecutorConstants.REG_RENAME)) {
                return new RenameCommand(communication, proxy, tokens[2], tokens[3]);

            } else if (command.equals(ExecutorConstants.REG_ALL_CACHED_FILES)) {
                poolExecutor = false;
                return new AllCachedFilesCommand(communication, proxy);

            } else if (command.equals(ExecutorConstants.REG_DELETE_CACHED_FILE)) {
                poolExecutor = false;
                return new DeleteCachedFileCommand(communication, proxy, tokens[2]);

            } else if (command.equals(ExecutorConstants.REG_EXIST)) {
                poolExecutor = false;
                return new ExistDataCommand(communication, proxy, tokens[2]);

                // Pool Operations
            } else if (command.equals(ExecutorConstants.POOL_ADD_OPERATION)) {
                poolExecutor = false;
                return new PoolAddOperationCommand(communication, proxy, tokens[2], tokens[3], tokens[4], tokens[5]);

            } else if (command.equals(ExecutorConstants.POOL_OPERATION_BY_ID)) {
                poolExecutor = false;
                return new PoolOperationByIdCommand(communication, proxy, tokens[2]);

            } else if (command.equals(ExecutorConstants.POOL_OPERATIONS_BY_USER)) {
                poolExecutor = false;
                return new PoolOperationsByUserCommand(communication, proxy, tokens[2]);

            } else if (command.equals(ExecutorConstants.POOL_REMOVE_OPERATION_BY_ID)) {
                poolExecutor = false;
                return new PoolRemoveOperationByIdCommand(communication, proxy, tokens[2]);

            } else if (command.equals(ExecutorConstants.POOL_REMOVE_OPERATIONS_BY_USER)) {
                poolExecutor = false;
                return new PoolRemoveOperationsByUserCommand(communication, proxy, tokens[2]);

            } else if (command.equals(ExecutorConstants.POOL_ALL_OPERATIONS)) {
                poolExecutor = false;
                return new PoolAllOperationsCommand(communication, proxy);

            } else if (command.equals(ExecutorConstants.POOL_CLEAR_DELETE_OPERATIONS)) {
                poolExecutor = false;
                return new PoolClearDeleteOperationsCommand(communication, proxy);

            } else {
                logException(new Exception("Command not recognized: " + tokens));
            }

        } catch (ArrayIndexOutOfBoundsException ex) {
            logException(new Exception("Wrong number of parameters."));
        }
        return null;
    }

    private void logException(Exception ex) {
        communication.sendMessage(Constants.ERROR + ex.getMessage());
        communication.sendMessage(Constants.END_OF_MESSAGE);

        logger.error(ex.getMessage());
        if (logger.isDebugEnabled()) {
            for (StackTraceElement stack : ex.getStackTrace()) {
                logger.debug(stack);
            }
        }
    }
}
