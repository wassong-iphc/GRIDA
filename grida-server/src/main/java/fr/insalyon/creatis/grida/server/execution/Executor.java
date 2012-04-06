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

import fr.insalyon.creatis.grida.common.Communication;
import fr.insalyon.creatis.grida.common.Constants;
import fr.insalyon.creatis.grida.common.ExecutorConstants;
import fr.insalyon.creatis.grida.server.execution.cache.AllCachedFilesCommand;
import fr.insalyon.creatis.grida.server.execution.cache.DeleteCachedFileCommand;
import fr.insalyon.creatis.grida.server.execution.command.*;
import fr.insalyon.creatis.grida.server.execution.pool.*;
import fr.insalyon.creatis.grida.server.execution.zombie.DeleteZombieFileCommand;
import fr.insalyon.creatis.grida.server.execution.zombie.ZombieGetListCommand;
import java.io.IOException;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class Executor extends Thread {

    private static final Logger logger = Logger.getLogger(Executor.class);
    private Communication communication;

    public Executor(Communication communication) {
        this.communication = communication;
    }

    @Override
    public void run() {

        try {
            String message = communication.getMessage();
            if (message != null) {
                Command command = parseCommand(message);

                if (command != null) {
                    command.execute();
                }
            } else {
                logException(new Exception("Error during message receive: " + message));
            }
        } catch (IOException ex) {
            logger.error(ex);
        } finally {
            try {
                communication.close();
            } catch (IOException ex) {
                logger.error(ex);
            }
        }
    }

    private Command parseCommand(String message) {

        try {
            String[] tokens = message.split(Constants.MSG_SEP_1);
            int command = new Integer(tokens[0]);
            String proxy = tokens[1];

            switch (command) {

                case ExecutorConstants.COM_GET_REMOTE_FILE:
                    return new GetRemoteFileCommand(communication, proxy, tokens[2], tokens[3]);

                case ExecutorConstants.COM_GET_REMOTE_FOLDER:
                    return new GetRemoteFolderCommand(communication, proxy, tokens[2], tokens[3]);

                case ExecutorConstants.COM_LIST_FILES_AND_FOLDERS:
                    return new ListFilesAndFoldersCommand(communication, proxy, tokens[2], tokens[3]);

                case ExecutorConstants.COM_GET_MODIFICATION_DATE:
                    return new GetModificationDateCommand(communication, proxy, tokens[2].split(Constants.MSG_SEP_2));

                case ExecutorConstants.COM_UPLOAD_FILE:
                    return new UploadFileCommand(communication, proxy, tokens[2], tokens[3]);

                case ExecutorConstants.COM_UPLOAD_FILE_TO_SES:
                    return new UploadFileCommand(communication, proxy, tokens[2], tokens[3], tokens[4].split(Constants.MSG_SEP_2));

                case ExecutorConstants.COM_REPLICATE_PREFERRED_SES:
                    return new ReplicatePreferredSEsCommand(communication, proxy, tokens[2]);

                case ExecutorConstants.COM_DELETE:
                    return new DeleteCommand(communication, proxy, tokens[2].split(Constants.MSG_SEP_2));

                case ExecutorConstants.COM_CREATE_FOLDER:
                    return new CreateFolderCommand(communication, proxy, tokens[2]);

                case ExecutorConstants.COM_RENAME:
                    return new RenameCommand(communication, proxy, tokens[2], tokens[3]);

                case ExecutorConstants.COM_EXIST:
                    return new ExistDataCommand(communication, proxy, tokens[2]);

                // Cache Operations
                case ExecutorConstants.CACHE_LIST_FILES:
                    return new AllCachedFilesCommand(communication, proxy);

                case ExecutorConstants.CACHE_DELETE_FILE:
                    return new DeleteCachedFileCommand(communication, proxy, tokens[2]);

                // Pool Operations
                case ExecutorConstants.POOL_ADD_OPERATION:
                    return new PoolAddOperationCommand(communication, proxy, tokens[2], tokens[3], tokens[4], tokens[5]);

                case ExecutorConstants.POOL_OPERATION_BY_ID:
                    return new PoolOperationByIdCommand(communication, proxy, tokens[2]);

                case ExecutorConstants.POOL_OPERATIONS_BY_USER:
                    return new PoolOperationsByUserCommand(communication, proxy, tokens[2]);

                case ExecutorConstants.POOL_REMOVE_OPERATION_BY_ID:
                    return new PoolRemoveOperationByIdCommand(communication, proxy, tokens[2]);

                case ExecutorConstants.POOL_REMOVE_OPERATIONS_BY_USER:
                    return new PoolRemoveOperationsByUserCommand(communication, proxy, tokens[2]);

                case ExecutorConstants.POOL_ALL_OPERATIONS:
                    return new PoolAllOperationsCommand(communication, proxy);

                case ExecutorConstants.POOL_LIMITED_OPERATIONS_BY_DATE:
                    return new PoolLimitedOperationsByDateCommand(communication, proxy, tokens[2], tokens[3], tokens[4]);

                // Zombie Operations
                case ExecutorConstants.ZOM_GET:
                    return new ZombieGetListCommand(communication, proxy);

                case ExecutorConstants.ZOM_DELETE:
                    return new DeleteZombieFileCommand(communication, proxy, tokens[2]);

                default:
                    logException(new Exception("Command not recognized: " + message));
            }

        } catch (NumberFormatException ex) {
            logException(new Exception("Invalid command: " + ex.getMessage()));
        } catch (ArrayIndexOutOfBoundsException ex) {
            logException(new Exception("Wrong number of parameters."));
        }
        return null;
    }

    private void logException(Exception ex) {

        communication.sendErrorMessage(ex.getMessage());
        communication.sendEndOfMessage();

        logger.error(ex.getMessage());
        if (logger.isDebugEnabled()) {
            for (StackTraceElement stack : ex.getStackTrace()) {
                logger.debug(stack);
            }
        }
    }
}
