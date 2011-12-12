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
import fr.insalyon.creatis.agent.vlet.common.bean.CachedFile;
import fr.insalyon.creatis.agent.vlet.common.bean.GridData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Rafael Silva
 */
public class VletAgentClient {

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
    public VletAgentClient(String host, int port, String proxyPath) {
        this.host = host;
        this.port = port;
        this.proxyPath = proxyPath;
    }

    /**
     * Downloads a remote file to a specific directory.
     *
     * @param remoteFile Remote file path
     * @param localDir Local directory path where the file should be downloaded
     * @return Local path of the downloaded file
     * @throws VletAgentClientException
     */
    public String getRemoteFile(String remoteFile, String localDir) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.REG_GET_REMOTE_FILE + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + Util.removeLfnFromPath(remoteFile) + Constants.SEPARATOR
                + localDir);

        String localFilePath = communication.getMessage();
        communication.close();

        return localFilePath.toString();
    }

    /**
     * Downloads a remote folder to a specific directory.
     *
     * @param remoteDir Remote folder path
     * @param localDir Local directory path where the folder should be downloaded
     * @return Local path of the downloaded folder
     * @throws VletAgentClientException
     */
    public String getRemoteFolder(String remoteDir, String localDir) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.REG_GET_REMOTE_FOLDER + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + Util.removeLfnFromPath(remoteDir) + Constants.SEPARATOR
                + localDir);

        String localFolderPath = communication.getMessage();
        communication.close();

        return localFolderPath.toString();
    }

    /**
     * Gets a list of files and folders from a directory.
     * 
     * @param dir Path of the directory
     * @param refresh Tells if the server should try to read data from cache
     * @return List of files and folders from a directory
     * @throws VletAgentClientException 
     */
    public List<GridData> getFolderData(String dir, boolean refresh) throws VletAgentClientException {

        try {
            Communication communication = new Communication(host, port);
            communication.sendMessage(
                    ExecutorConstants.REG_LIST_FILES_AND_FOLDERS + Constants.SEPARATOR
                    + proxyPath + Constants.SEPARATOR
                    + Util.removeLfnFromPath(dir) + Constants.SEPARATOR
                    + refresh);

            String filesAndFolders = communication.getMessage();
            communication.close();

            List<GridData> filesList = new ArrayList<GridData>();
            String SEP = "--";
            if (!filesAndFolders.isEmpty()) {
                for (String data : filesAndFolders.split(Constants.SEPARATOR)) {
                    String[] d = data.split(SEP);
                    if (d[1].equals(GridData.Type.Folder.name())) {
                        filesList.add(new GridData(d[0], GridData.Type.Folder, d[2]));

                    } else {
                        filesList.add(new GridData(d[0], GridData.Type.File, new Long(d[2]), d[3], d[4], d[5]));
                    }
                }
            }

            return filesList;
        } catch (ArrayIndexOutOfBoundsException ex) {
            throw new VletAgentClientException("Wrong number of parameters from server response.");
        }
    }

    /**
     * Gets the modification date of the provided file.
     * 
     * @param fileName File name to get the modification date
     * @return Modification date of the file
     * @throws VletAgentClientException
     */
    public Long getModificationDate(String fileName) throws VletAgentClientException {

        List<String> filesList = new ArrayList<String>();
        filesList.add(fileName);

        return getModificationDate(filesList).get(0);
    }

    /**
     * Gets the modification dates for the list of files provided.
     *
     * @param filesList List of files to get the modification date
     * @return List of modification dates respectively to the list of files
     * @throws VletAgentClientException
     */
    public List<Long> getModificationDate(List<String> filesList) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        StringBuilder sb = new StringBuilder();
        for (String fileName : filesList) {
            if (!sb.toString().isEmpty()) {
                sb.append(Constants.INTRA_SEPARATOR);
            }
            sb.append(Util.removeLfnFromPath(fileName));
        }
        communication.sendMessage(
                ExecutorConstants.REG_GET_MODIFICATION_DATE + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + sb.toString());

        String dates = communication.getMessage();
        communication.close();

        List<Long> datesList = new ArrayList<Long>();
        for (String date : dates.split(Constants.SEPARATOR)) {
            datesList.add(new Long(date));
        }

        return datesList;
    }

    /**
     * Uploads a local file to a specific remote directory.
     *
     * @param localFile Local file path
     * @param remoteDir Remote directory path where the file should be uploaded
     * @return Remote path of the uploaded file
     * @throws VletAgentClientException
     */
    public String uploadFile(String localFile, String remoteDir) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.REG_UPLOAD_FILE + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + localFile + Constants.SEPARATOR
                + Util.removeLfnFromPath(remoteDir));

        String localFilePath = communication.getMessage();
        communication.close();

        return localFilePath.toString();
    }

    /**
     * Uploads a local file to a specific remote directory and a specific SE.
     *
     * @param localFile Local file path
     * @param remoteDir Remote directory path where the file should be uploaded
     * @param storageElement Storage Element host
     * @return Remote path of the uploaded file
     * @throws VletAgentClientException
     */
    public String uploadFileToSE(String localFile, String remoteDir, String storageElement) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.REG_UPLOAD_FILE_TO_SES + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + localFile + Constants.SEPARATOR
                + Util.removeLfnFromPath(remoteDir) + Constants.SEPARATOR
                + storageElement);

        String localFilePath = communication.getMessage();
        communication.close();

        return localFilePath.toString();
    }

    /**
     * Uploads a local file to a specific remote directory and a list of SEs.
     *
     * @param localFile Local file path
     * @param remoteDir Remote directory path where the file should be uploaded
     * @param storageElementsList List of Storage Elements
     * @return Remote path of the uploaded file
     * @throws VletAgentClientException
     */
    public String uploadFileToSE(String localFile, String remoteDir, List<String> storageElementsList) throws VletAgentClientException {

        Communication communication = new Communication(host, port);

        StringBuilder storageElements = new StringBuilder();
        for (String se : storageElementsList) {
            if (storageElements.length() > 0) {
                storageElements.append(Constants.INTRA_SEPARATOR);
            }
            storageElements.append(se);
        }

        communication.sendMessage(
                ExecutorConstants.REG_UPLOAD_FILE_TO_SES + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + localFile + Constants.SEPARATOR
                + Util.removeLfnFromPath(remoteDir) + Constants.SEPARATOR
                + storageElements.toString());

        String localFilePath = communication.getMessage();
        communication.close();

        return localFilePath.toString();
    }

    /**
     * Replicates a remote file to a list of preferred SEs declared in the agent.
     *
     * @param remoteFile Remote file name to be replicated
     * @throws VletAgentClientException
     */
    public void replicateToPreferredSEs(String remoteFile) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.REG_REPLICATE_PREFERRED_SES + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + Util.removeLfnFromPath(remoteFile));

        communication.getMessage();
        communication.close();
    }

    /**
     * Deletes a file or a directory.
     * 
     * @param path Path to be deleted
     * @throws VletAgentClientException
     */
    public void delete(String path) throws VletAgentClientException {
        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.REG_DELETE + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + Util.removeLfnFromPath(path));

        communication.getMessage();
        communication.close();
    }

    /**
     * Deletes a list of remote files or directories.
     *
     * @param paths List of remote files or directories to be deleted.
     * @throws VletAgentClientException
     */
    public void delete(List<String> paths) throws VletAgentClientException {

        StringBuilder files = new StringBuilder();
        for (String file : paths) {
            if (files.length() > 0) {
                files.append(Constants.INTRA_SEPARATOR);
            }
            files.append(Util.removeLfnFromPath(file));
        }
        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.REG_DELETE + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + files.toString());

        communication.getMessage();
        communication.close();
    }

    /**
     * Creates a new directory.
     *
     * @param path Path where the new directory will be created
     * @param dirName Name of the new directory
     * @throws VletAgentClientException
     */
    public void createDirectory(String path, String dirName) throws VletAgentClientException {
        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.REG_CREATE_DIRECTORY + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + Util.removeLfnFromPath(path + "/" + dirName));

        communication.getMessage();
        communication.close();
    }

    /**
     * Renames a file/folder.
     * 
     * @param oldPath
     * @param newPath
     * @throws VletAgentClientException 
     */
    public void rename(String oldPath, String newPath) throws VletAgentClientException {
        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.REG_RENAME + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + Util.removeLfnFromPath(oldPath) + Constants.SEPARATOR
                + newPath);

        communication.getMessage();
        communication.close();
    }

    /**
     * Verify if a file or folder exists.
     * 
     * @param remotePath
     * @return
     * @throws VletAgentClientException If some data does not exist
     */
    public boolean exist(String remotePath) throws VletAgentClientException {

        try {
            Communication communication = new Communication(host, port);
            communication.sendMessage(
                    ExecutorConstants.REG_EXIST + Constants.SEPARATOR
                    + proxyPath + Constants.SEPARATOR
                    + Util.removeLfnFromPath(remotePath));

            communication.getMessage();
            communication.close();
            return true;

        } catch (VletAgentClientException ex) {
            if (ex.getMessage().contains("The following data does not exist")) {
                return false;
            } else {
                throw ex;
            }
        }
    }

    /**
     * Gets a list of all cached files.
     *
     * @return List of all cached files
     * @throws VletAgentClientException
     */
    public List<CachedFile> getCachedFiles() throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.REG_ALL_CACHED_FILES + Constants.SEPARATOR
                + proxyPath);

        String cachedFiles = communication.getMessage();
        communication.close();

        List<CachedFile> cachedFilesList = new ArrayList<CachedFile>();

        if (!cachedFiles.isEmpty()) {
            for (String data : cachedFiles.split(Constants.SEPARATOR)) {
                String[] cachedFile = data.split(Constants.INTRA_SEPARATOR);
                cachedFilesList.add(new CachedFile(
                        cachedFile[0], cachedFile[1],
                        Double.valueOf(cachedFile[2]), Integer.valueOf(cachedFile[3]),
                        new Date(Long.valueOf(cachedFile[4]))));
            }
        }

        return cachedFilesList;
    }

    /**
     * Deletes a cached file.
     *
     * @param path Grid path
     * @throws VletAgentClientException
     */
    public void deleteCachedFile(String path) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.REG_DELETE_CACHED_FILE + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR + path);

        communication.getMessage();
        communication.close();
    }

    /**
     * Deletes a list of remote files
     * @deprecated
     *
     * @param remoteFileList List of remote files to be deleted.
     * @throws VletAgentClientException
     * 
     * @see delete(List&lt;String&gt; paths)
     */
    public void deleteFiles(List<String> remoteFilesList) throws VletAgentClientException {
        delete(remoteFilesList);
    }

    /**
     * @deprecated 
     * Gets a list of files and folders from a directory.
     *
     * @param dir Path of the directory
     * @param refresh Tells if the server should try to read data from cache
     * @return List of files and folders from a directory in the format '<basename>--[Folder|File]'
     * @throws VletAgentClientException
     * 
     * @see getFolderData(String dir, boolean refresh)
     */
    public List<String> getFilesAndFoldersList(String dir, boolean refresh) throws VletAgentClientException {

        Communication communication = new Communication(host, port);
        communication.sendMessage(
                ExecutorConstants.REG_LIST_FILES_AND_FOLDERS + Constants.SEPARATOR
                + proxyPath + Constants.SEPARATOR
                + Util.removeLfnFromPath(dir) + Constants.SEPARATOR
                + refresh);

        String filesAndFolders = communication.getMessage();
        communication.close();

        List<String> filesList = new ArrayList<String>();
        filesList.addAll(Arrays.asList(filesAndFolders.split(Constants.SEPARATOR)));

        return filesList;
    }
}
