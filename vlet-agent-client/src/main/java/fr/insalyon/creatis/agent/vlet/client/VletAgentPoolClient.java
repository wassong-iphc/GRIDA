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
package fr.insalyon.creatis.agent.vlet.client;

import fr.insalyon.creatis.agent.vlet.common.Constants;
import fr.insalyon.creatis.agent.vlet.common.ExecutorConstants;
import fr.insalyon.creatis.agent.vlet.common.bean.Operation;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Rafael Silva
 */
public class VletAgentPoolClient {

    private String host;
    private int port;
    private String proxyPath;

    /**
     * Creates an instance of the Vlet Agent Client.
     *
     * @param host Vlet Agent host
     * @param port Vlet Agent port
     * @param proxyPath Path of the user's proxy file
     */
    public VletAgentPoolClient(String host, int port, String proxyPath) {
        this.host = host;
        this.port = port;
        this.proxyPath = proxyPath;
    }

    /**
     *
     * Adds an upload operation to the server pool.
     *
     * @param localFile Local file path
     * @param remoteDir Remote directory path where the file should be uploaded
     * @param user User name
     * @return ID of the operation
     * @throws VletAgentClientException
     */
    public String uploadFile(String localFile, String remoteDir, String user) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.POOL_ADD_OPERATION + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + localFile + Constants.SEPARATOR
                + Util.removeLfnFromPath(remoteDir) + Constants.SEPARATOR
                + Operation.Type.Upload.name() + Constants.SEPARATOR
                + user);

        String operationID = communication.getMessage();
        communication.close();

        return operationID;
    }

    /**
     * Downloads a remote file to a specific directory.
     *
     * @param remoteFile Remote file path
     * @param localDir Local directory path where the file should be downloaded
     * @param user User name
     * @return ID of the operation
     * @throws VletAgentClientException
     */
    public String downloadFile(String remoteFile, String localDir, String user) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.POOL_ADD_OPERATION + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + Util.removeLfnFromPath(remoteFile) + Constants.SEPARATOR
                + localDir + Constants.SEPARATOR
                + Operation.Type.Download.name() + Constants.SEPARATOR
                + user);

        String operationID = communication.getMessage();
        communication.close();

        return operationID;
    }

    /**
     * Downloads an array of remote files to a zip file.
     * 
     * @param remoteFiles Remote files path array.
     * @param packName Path and name of the zip file (e.g.: /tmp/zipfile)
     * @param user User name
     * @return ID of the operation
     * @throws VletAgentClientException 
     */
    public String downloadFiles(String[] remoteFiles, String packName, String user) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.POOL_ADD_OPERATION + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + Util.parseArrayToString(Util.removeLfnFromPath(remoteFiles))
                + Constants.SEPARATOR
                + packName + Constants.SEPARATOR
                + Operation.Type.Download_Files.name() + Constants.SEPARATOR
                + user);

        String operationID = communication.getMessage();
        communication.close();

        return operationID;
    }

    /**
     * Downloads a remote folder to a specific directory.
     * 
     * @param remoteFolder Remote folder path
     * @param localDir Local directory path where the folder should be downloaded
     * @param user User name
     * @return ID of the operation
     * @throws VletAgentClientException 
     */
    public String downloadFolder(String remoteFolder, String localDir, String user) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.POOL_ADD_OPERATION + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + Util.removeLfnFromPath(remoteFolder) + Constants.SEPARATOR
                + localDir + Constants.SEPARATOR
                + Operation.Type.Download.name() + Constants.SEPARATOR
                + user);

        String operationID = communication.getMessage();
        communication.close();

        return operationID;
    }

    /**
     * Replicates a remote file to a list of preferred SEs declared in the agent.
     * 
     * @param remoteFile Remote file path
     * @param user User name
     * @return ID of the operation
     * @throws VletAgentClientException 
     */
    public String replicateToPreferredSEs(String remoteFile, String user) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.POOL_ADD_OPERATION + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + Util.removeLfnFromPath(remoteFile) + Constants.SEPARATOR
                + "" + Constants.SEPARATOR
                + Operation.Type.Replicate.name() + Constants.SEPARATOR
                + user);

        String operationID = communication.getMessage();
        communication.close();

        return operationID;
    }

    /**
     * Deletes a remote file or folder
     *
     * @param remotePath Remote file/folder path
     * @param user User name
     * @throws VletAgentClientException
     */
    public void delete(String remotePath, String user) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.POOL_ADD_OPERATION + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + Util.removeLfnFromPath(remotePath) + Constants.SEPARATOR
                + "" + Constants.SEPARATOR
                + Operation.Type.Delete.name() + Constants.SEPARATOR
                + user);
    }

    /**
     * Gets a list of operations by user.
     *
     * @param user User name to be filtered
     * @return List of operations associated to the user
     * @throws VletAgentClientException
     */
    public List<Operation> getOperationsListByUser(String user) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.POOL_OPERATIONS_BY_USER + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR + user);

        String operations = communication.getMessage();
        communication.close();

        List<Operation> operationsList = new ArrayList<Operation>();

        if (!operations.isEmpty()) {
            for (String data : operations.split(Constants.SEPARATOR)) {
                String[] operation = data.split(Constants.INTRA_SEPARATOR);
                operationsList.add(new Operation(
                        operation[0], new Date(new Long(operation[1])), operation[2],
                        operation[3], operation[4], operation[5], operation[6]));
            }
        }

        return operationsList;
    }

    /**
     * Gets an operation according to ID.
     *
     * @param id Operation identification
     * @return Operation object
     * @throws VletAgentClientException
     */
    public Operation getOperationById(String id) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.POOL_OPERATION_BY_ID + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR + id);

        String data = communication.getMessage();
        communication.close();

        String[] operation = data.split(Constants.INTRA_SEPARATOR);

        return new Operation(
                operation[0], new Date(new Long(operation[1])), operation[2],
                operation[3], operation[4], operation[5], operation[6]);
    }

    /**
     * Removes an operation according to ID.
     *
     * @param id Operation identification
     * @throws VletAgentClientException
     */
    public void removeOperationById(String id) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.POOL_REMOVE_OPERATION_BY_ID + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR + id);

        communication.getMessage();
        communication.close();
    }

    /**
     * Removes all operations of a user.
     *
     * @param user User name
     * @throws VletAgentClientException
     */
    public void removeOperationsByUser(String user) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.POOL_REMOVE_OPERATIONS_BY_USER + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR + user);

        communication.getMessage();
        communication.close();
    }

    /**
     * Gets a list of all operations.
     *
     * @return List of all operations
     * @throws VletAgentClientException
     */
    public List<Operation> getAllOperations() throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.POOL_ALL_OPERATIONS + Constants.SEPARATOR
                + proxyPath);

        String operations = communication.getMessage();
        communication.close();

        List<Operation> operationsList = new ArrayList<Operation>();

        if (!operations.isEmpty()) {
            for (String data : operations.split(Constants.SEPARATOR)) {
                String[] operation = data.split(Constants.INTRA_SEPARATOR);
                operationsList.add(new Operation(
                        operation[0], new Date(new Long(operation[1])), operation[2],
                        operation[3], operation[4], operation[5], operation[6]));
            }
        }

        return operationsList;
    }

    /**
     * Clears all finished delete operations.
     * 
     * @throws VletAgentClientException 
     */
    public void clearDeleteOperations() throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.POOL_CLEAR_DELETE_OPERATIONS + Constants.SEPARATOR
                + proxyPath);

        communication.getMessage();
        communication.close();
    }
}
