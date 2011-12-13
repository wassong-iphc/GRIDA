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
import fr.insalyon.creatis.grida.common.bean.GridData;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Rafael Silva
 */
public class GRIDAClient extends AbstractGRIDAClient {

    /**
     * Creates an instance of the GRIDA Client.
     *
     * @param host GRIDA server host
     * @param port GRIDA server port
     * @param proxyPath Path of the user's proxy file
     */
    public GRIDAClient(String host, int port, String proxyPath) {
        
        super(host, port, proxyPath);
    }

    /**
     * Downloads a remote file to a specific directory.
     *
     * @param remoteFile Remote file path
     * @param localDir Local directory path where the file should be downloaded
     * @return Local path of the downloaded file
     * @throws GRIDAClientException
     */
    public String getRemoteFile(String remoteFile, String localDir) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.COM_GET_REMOTE_FILE + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(remoteFile) + Constants.MSG_SEP_1
                    + localDir);
            communication.sendEndOfMessage();

            String localFilePath = communication.getMessage();
            communication.close();

            return localFilePath.toString();

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Downloads a remote folder to a specific directory.
     *
     * @param remoteDir Remote folder path
     * @param localDir Local directory path where the folder should be downloaded
     * @return Local path of the downloaded folder
     * @throws GRIDAClientException
     */
    public String getRemoteFolder(String remoteDir, String localDir) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.COM_GET_REMOTE_FOLDER + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(remoteDir) + Constants.MSG_SEP_1
                    + localDir);
            communication.sendEndOfMessage();

            String localFolderPath = communication.getMessage();
            communication.close();

            return localFolderPath.toString();

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Gets a list of files and folders from a directory.
     * 
     * @param dir Path of the directory
     * @param refresh Tells if the server should try to read data from cache
     * @return List of files and folders from a directory
     * @throws GRIDAClientException 
     */
    public List<GridData> getFolderData(String dir, boolean refresh) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.COM_LIST_FILES_AND_FOLDERS + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(dir) + Constants.MSG_SEP_1
                    + refresh);
            communication.sendEndOfMessage();

            String filesAndFolders = communication.getMessage();
            communication.close();

            List<GridData> filesList = new ArrayList<GridData>();

            if (!filesAndFolders.isEmpty()) {
                for (String data : filesAndFolders.split(Constants.MSG_SEP_1)) {
                    String[] d = data.split(Constants.MSG_SEP_2);
                    if (d[1].equals(GridData.Type.Folder.name())) {
                        filesList.add(new GridData(d[0], GridData.Type.Folder, d[2]));

                    } else {
                        filesList.add(new GridData(d[0], GridData.Type.File, new Long(d[2]), d[3], d[4], d[5]));
                    }
                }
            }
            return filesList;

        } catch (ArrayIndexOutOfBoundsException ex) {
            throw new GRIDAClientException("Wrong number of parameters from server response.");
        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Gets the modification date of the provided file.
     * 
     * @param fileName File name to get the modification date
     * @return Modification date of the file
     * @throws GRIDAClientException
     */
    public Long getModificationDate(String fileName) throws GRIDAClientException {

        List<String> filesList = new ArrayList<String>();
        filesList.add(fileName);

        return getModificationDate(filesList).get(0);
    }

    /**
     * Gets the modification dates for the list of files provided.
     *
     * @param filesList List of files to get the modification date
     * @return List of modification dates respectively to the list of files
     * @throws GRIDAClientException
     */
    public List<Long> getModificationDate(List<String> filesList) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();

            StringBuilder sb = new StringBuilder();
            for (String fileName : filesList) {
                if (!sb.toString().isEmpty()) {
                    sb.append(Constants.MSG_SEP_2);
                }
                sb.append(Util.removeLfnFromPath(fileName));
            }
            communication.sendMessage(
                    ExecutorConstants.COM_GET_MODIFICATION_DATE + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1 + sb.toString());
            communication.sendEndOfMessage();

            String dates = communication.getMessage();
            communication.close();

            List<Long> datesList = new ArrayList<Long>();
            for (String date : dates.split(Constants.MSG_SEP_1)) {
                datesList.add(new Long(date));
            }

            return datesList;

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Uploads a local file to a specific remote directory.
     *
     * @param localFile Local file path
     * @param remoteDir Remote directory path where the file should be uploaded
     * @return Remote path of the uploaded file
     * @throws GRIDAClientException
     */
    public String uploadFile(String localFile, String remoteDir) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.COM_UPLOAD_FILE + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + localFile + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(remoteDir));
            communication.sendEndOfMessage();

            String localFilePath = communication.getMessage();
            communication.close();

            return localFilePath.toString();

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Uploads a local file to a specific remote directory and a specific SE.
     *
     * @param localFile Local file path
     * @param remoteDir Remote directory path where the file should be uploaded
     * @param storageElement Storage Element host
     * @return Remote path of the uploaded file
     * @throws GRIDAClientException
     */
    public String uploadFileToSE(String localFile, String remoteDir,
            String storageElement) throws GRIDAClientException {

        List<String> storageElements = new ArrayList<String>();
        storageElements.add(storageElement);

        return uploadFileToSE(localFile, remoteDir, storageElements);
    }

    /**
     * Uploads a local file to a specific remote directory and a list of SEs.
     *
     * @param localFile Local file path
     * @param remoteDir Remote directory path where the file should be uploaded
     * @param storageElementsList List of Storage Elements
     * @return Remote path of the uploaded file
     * @throws GRIDAClientException
     */
    public String uploadFileToSE(String localFile, String remoteDir,
            List<String> storageElementsList) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();

            StringBuilder storageElements = new StringBuilder();
            for (String se : storageElementsList) {
                if (storageElements.length() > 0) {
                    storageElements.append(Constants.MSG_SEP_2);
                }
                storageElements.append(se);
            }

            communication.sendMessage(
                    ExecutorConstants.COM_UPLOAD_FILE_TO_SES + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + localFile + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(remoteDir) + Constants.MSG_SEP_1
                    + storageElements.toString());
            communication.sendEndOfMessage();

            String localFilePath = communication.getMessage();
            communication.close();

            return localFilePath.toString();

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Replicates a remote file to a list of preferred SEs declared in the agent.
     *
     * @param remoteFile Remote file name to be replicated
     * @throws GRIDAClientException
     */
    public void replicateToPreferredSEs(String remoteFile) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.COM_REPLICATE_PREFERRED_SES + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(remoteFile));
            communication.sendEndOfMessage();

            communication.getMessage();
            communication.close();

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Deletes a file or a directory.
     * 
     * @param path Path to be deleted
     * @throws GRIDAClientException
     */
    public void delete(String path) throws GRIDAClientException {

        List<String> paths = new ArrayList<String>();
        paths.add(path);

        delete(paths);
    }

    /**
     * Deletes a list of remote files or directories.
     *
     * @param paths List of remote files or directories to be deleted.
     * @throws GRIDAClientException
     */
    public void delete(List<String> paths) throws GRIDAClientException {

        try {
            StringBuilder files = new StringBuilder();
            for (String file : paths) {
                if (files.length() > 0) {
                    files.append(Constants.MSG_SEP_2);
                }
                files.append(Util.removeLfnFromPath(file));
            }
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.COM_DELETE + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + files.toString());
            communication.sendEndOfMessage();

            communication.getMessage();
            communication.close();

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Creates a new folder.
     *
     * @param path Path where the new folder will be created
     * @param folderName Name of the new folder
     * @throws GRIDAClientException
     */
    public void createFolder(String path, String folderName) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.COM_CREATE_FOLDER + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(path + "/" + folderName));
            communication.sendEndOfMessage();

            communication.getMessage();
            communication.close();

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Renames a file/folder.
     * 
     * @param oldPath
     * @param newPath
     * @throws GRIDAClientException 
     */
    public void rename(String oldPath, String newPath) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.COM_RENAME + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(oldPath) + Constants.MSG_SEP_1
                    + newPath);
            communication.sendEndOfMessage();

            communication.getMessage();
            communication.close();

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Verify if a file or folder exists.
     * 
     * @param remotePath
     * @return
     * @throws GRIDAClientException If some data does not exist
     */
    public boolean exist(String remotePath) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.COM_EXIST + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1
                    + Util.removeLfnFromPath(remotePath));
            communication.sendEndOfMessage();

            boolean exist = Boolean.valueOf(communication.getMessage());
            communication.close();
            
            return exist;

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }
}
