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
package fr.insalyon.creatis.grida.client;

import fr.insalyon.creatis.grida.common.Communication;
import fr.insalyon.creatis.grida.common.Constants;
import fr.insalyon.creatis.grida.common.ExecutorConstants;
import fr.insalyon.creatis.grida.common.bean.Operation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Rafael Silva
 */
public class GRIDAPoolClient extends AbstractGRIDAClient {

    /**
     * Creates an instance of the GRIDA pool client.
     *
     * @param host GRIDA server host
     * @param port GRIDA server port
     * @param proxyPath Path of the user's proxy file
     */
    public GRIDAPoolClient(String host, int port, String proxyPath) {

        super(host, port, proxyPath);
    }

    /**
     *
     * Adds an upload operation to the server pool.
     *
     * @param localFile Local file path
     * @param remoteDir Remote directory path where the file should be uploaded
     * @param user User name
     * @return ID of the operation
     * @throws GRIDAClientException
     */
    public String uploadFile(String localFile, String remoteDir, String user) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.POOL_ADD_OPERATION + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + localFile + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(remoteDir) + Constants.MSG_SEP_1
                    + Operation.Type.Upload.name() + Constants.MSG_SEP_1
                    + user);
            communication.sendEndOfMessage();

            String operationID = communication.getMessage();
            communication.close();

            return operationID;

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Downloads a remote file to a specific directory.
     *
     * @param remoteFile Remote file path
     * @param localDir Local directory path where the file should be downloaded
     * @param user User name
     * @return ID of the operation
     * @throws GRIDAClientException
     */
    public String downloadFile(String remoteFile, String localDir, String user) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.POOL_ADD_OPERATION + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(remoteFile) + Constants.MSG_SEP_1
                    + localDir + Constants.MSG_SEP_1
                    + Operation.Type.Download.name() + Constants.MSG_SEP_1
                    + user);
            communication.sendEndOfMessage();

            String operationID = communication.getMessage();
            communication.close();

            return operationID;

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Downloads an array of remote files to a zip file.
     * 
     * @param remoteFiles Remote files path array.
     * @param packName Path and name of the zip file (e.g.: /tmp/zipfile)
     * @param user User name
     * @return ID of the operation
     * @throws GRIDAClientException 
     */
    public String downloadFiles(String[] remoteFiles, String packName, String user) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.POOL_ADD_OPERATION + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + Util.parseArrayToString(Util.removeLfnFromPath(remoteFiles))
                    + Constants.MSG_SEP_1
                    + packName + Constants.MSG_SEP_1
                    + Operation.Type.Download_Files.name() + Constants.MSG_SEP_1
                    + user);
            communication.sendEndOfMessage();

            String operationID = communication.getMessage();
            communication.close();

            return operationID;

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Downloads a remote folder to a specific directory.
     * 
     * @param remoteFolder Remote folder path
     * @param localDir Local directory path where the folder should be downloaded
     * @param user User name
     * @return ID of the operation
     * @throws GRIDAClientException 
     */
    public String downloadFolder(String remoteFolder, String localDir, String user) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.POOL_ADD_OPERATION + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(remoteFolder) + Constants.MSG_SEP_1
                    + localDir + Constants.MSG_SEP_1
                    + Operation.Type.Download.name() + Constants.MSG_SEP_1
                    + user);
            communication.sendEndOfMessage();

            String operationID = communication.getMessage();
            communication.close();

            return operationID;

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Replicates a remote file to a list of preferred SEs declared in the agent.
     * 
     * @param remoteFile Remote file path
     * @param user User name
     * @return ID of the operation
     * @throws GRIDAClientException 
     */
    public String replicateToPreferredSEs(String remoteFile, String user) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.POOL_ADD_OPERATION + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(remoteFile) + Constants.MSG_SEP_1
                    + "" + Constants.MSG_SEP_1
                    + Operation.Type.Replicate.name() + Constants.MSG_SEP_1
                    + user);
            communication.sendEndOfMessage();

            String operationID = communication.getMessage();
            communication.close();

            return operationID;

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Deletes a remote file or folder
     *
     * @param remotePath Remote file/folder path
     * @param user User name
     * @throws GRIDAClientException
     */
    public void delete(String remotePath, String user) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.POOL_ADD_OPERATION + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(remotePath) + Constants.MSG_SEP_1
                    + "" + Constants.MSG_SEP_1
                    + Operation.Type.Delete.name() + Constants.MSG_SEP_1
                    + user);
            communication.sendEndOfMessage();

            communication.getMessage();
            communication.close();

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Gets a list of operations by user.
     *
     * @param user User name to be filtered
     * @return List of operations associated to the user
     * @throws GRIDAClientException
     */
    public List<Operation> getOperationsListByUser(String user) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.POOL_OPERATIONS_BY_USER + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1 + user);
            communication.sendEndOfMessage();

            String operations = communication.getMessage();
            communication.close();

            List<Operation> operationsList = new ArrayList<Operation>();

            if (!operations.isEmpty()) {
                for (String data : operations.split(Constants.MSG_SEP_1)) {
                    String[] operation = data.split(Constants.MSG_SEP_2);
                    operationsList.add(new Operation(
                            operation[0], new Date(new Long(operation[1])), operation[2],
                            operation[3], operation[4], operation[5], operation[6]));
                }
            }

            return operationsList;

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Gets a limited list of operations by user and date.
     * 
     * @param user User name to be filtered
     * @param limit Maximum size of the list
     * @param startDate Offset date
     * @return List of operations associated to the user
     * @throws GRIDAClientException 
     */
    public List<Operation> getOperationsLimitedListByUserAndDate(String user,
            int limit, Date startDate) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.POOL_LIMITED_OPERATIONS_BY_DATE
                    + Constants.MSG_SEP_1 + proxyPath
                    + Constants.MSG_SEP_1 + user
                    + Constants.MSG_SEP_1 + limit
                    + Constants.MSG_SEP_1 + startDate.getTime());
            communication.sendEndOfMessage();

            String operations = communication.getMessage();
            communication.close();

            List<Operation> operationsList = new ArrayList<Operation>();

            if (!operations.isEmpty()) {
                for (String data : operations.split(Constants.MSG_SEP_1)) {
                    String[] operation = data.split(Constants.MSG_SEP_2);
                    operationsList.add(new Operation(
                            operation[0], new Date(new Long(operation[1])), operation[2],
                            operation[3], operation[4], operation[5], operation[6]));
                }
            }

            return operationsList;

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Gets an operation according to ID.
     *
     * @param id Operation identification
     * @return Operation object
     * @throws GRIDAClientException
     */
    public Operation getOperationById(String id) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.POOL_OPERATION_BY_ID + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1 + id);
            communication.sendEndOfMessage();

            String data = communication.getMessage();
            communication.close();

            String[] operation = data.split(Constants.MSG_SEP_2);

            return new Operation(
                    operation[0], new Date(new Long(operation[1])), operation[2],
                    operation[3], operation[4], operation[5], operation[6]);

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Removes an operation according to ID.
     *
     * @param id Operation identification
     * @throws GRIDAClientException
     */
    public void removeOperationById(String id) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.POOL_REMOVE_OPERATION_BY_ID + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1 + id);
            communication.sendEndOfMessage();

            communication.getMessage();
            communication.close();

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Removes all operations of a user.
     *
     * @param user User name
     * @throws GRIDAClientException
     */
    public void removeOperationsByUser(String user) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.POOL_REMOVE_OPERATIONS_BY_USER + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1 + user);
            communication.sendEndOfMessage();

            communication.getMessage();
            communication.close();

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Gets a list of all operations.
     *
     * @return List of all operations
     * @throws GRIDAClientException
     */
    public List<Operation> getAllOperations() throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.POOL_ALL_OPERATIONS + Constants.MSG_SEP_1
                    + proxyPath);
            communication.sendEndOfMessage();

            String operations = communication.getMessage();
            communication.close();

            List<Operation> operationsList = new ArrayList<Operation>();

            if (!operations.isEmpty()) {
                for (String data : operations.split(Constants.MSG_SEP_1)) {
                    String[] operation = data.split(Constants.MSG_SEP_2);
                    operationsList.add(new Operation(
                            operation[0], new Date(new Long(operation[1])), operation[2],
                            operation[3], operation[4], operation[5], operation[6]));
                }
            }

            return operationsList;

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }
}
