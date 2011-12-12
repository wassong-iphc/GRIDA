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
package fr.insalyon.creatis.agent.vlet.execution.operation;

import fr.insalyon.creatis.agent.vlet.Configuration;
import fr.insalyon.creatis.agent.vlet.common.bean.CachedFile;
import fr.insalyon.creatis.agent.vlet.dao.CacheFileDAO;
import fr.insalyon.creatis.agent.vlet.dao.DAOFactory;
import fr.insalyon.creatis.devtools.zip.FolderZipper;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import nl.uva.vlet.vfs.VDir;
import nl.uva.vlet.vfs.VFSClient;
import nl.uva.vlet.vfs.VFSNode;
import nl.uva.vlet.vfs.VFile;
import nl.uva.vlet.vrl.VRL;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class Operations {

    private final static Logger logger = Logger.getLogger(Operations.class);

    /**
     * Uploads a local file to a remote folder.
     *
     * @param proxy User's proxy
     * @param localFilePath Local file path
     * @param remoteDir Remote folder path
     * @return Remote file path
     * @throws Exception
     */
    public static String uploadFile(String proxy, String localFilePath,
            String remoteDir) throws Exception {

        if (Configuration.getInstance().useLcgCommands()) {
            try {
                return LCGOperations.uploadFile(proxy, localFilePath, remoteDir);

            } catch (Exception ex) {
                logger.warn(ex.getMessage());
            }
        }
        return VletOperations.uploadFile(proxy, localFilePath, remoteDir);
    }

    /**
     * Downloads a remote file to a local folder.
     *
     * @param proxy User's proxy
     * @param localDirPath Local folder path
     * @param fileName Remote file name
     * @param remoteFilePath Remote file path
     * @return Local file path
     * @throws Exception
     */
    public static String downloadFile(String proxy, String localDirPath,
            String fileName, String remoteFilePath) throws Exception {

        File localDir = new File(localDirPath);
        if (!localDir.exists()) {
            localDir.mkdirs();
        }

        long destModificationDate = -1;

        if (Configuration.getInstance().useLcgCommands()) {

            File destFile = new File(localDir.getAbsolutePath() + "/" + fileName);
            if (destFile.exists()) {
                destModificationDate = destFile.lastModified();
            }

            long remoteModificationDate = LCGOperations.getModificationDate(proxy, remoteFilePath);

            if (remoteModificationDate > destModificationDate) {
                try {
                    return LCGOperations.downloadFile(proxy,
                            localDirPath, fileName, remoteFilePath);

                } catch (Exception ex) {
                    logger.warn("Failed to perform download from LCG command.");
                    try {
                        return LCGFailoverOperations.downloadFile(proxy,
                                localDirPath, fileName, remoteFilePath);

                    } catch (Exception ex1) {
                        logger.warn("Failed to perform download from failover servers.");
                    }
                }
            } else {
                logger.info("Avoiding download: file \"" + destFile.getAbsolutePath()
                        + "\" is up to date.");
                return destFile.getAbsolutePath();
            }
        }

        VletOperations.setProxy(proxy);
        VFSClient vfsClient = Configuration.getInstance().getVfsClient();
        VRL vrlRemote = new VRL("lfn://" + Configuration.LFC_HOST + remoteFilePath);
        VFile remoteFile = vfsClient.getFile(vrlRemote);
        VRL vrlDest = new VRL("file://" + localDir.getAbsolutePath() + "/" + fileName);

        VFile destFile = null;
        if (new File(localDir.getAbsolutePath() + "/" + fileName).exists()) {
            destFile = vfsClient.getFile(vrlDest);
            destModificationDate = destFile.getModificationTime();
        }

        if (remoteFile.getModificationTime() > destModificationDate) {
            return VletOperations.downloadFile(proxy, vrlRemote,
                    vrlDest, remoteFile, destFile, localDir);

        } else {
            logger.info("Avoiding download: file \"" + vrlDest.toString()
                    + "\" is up to date.");
        }

        return destFile.getPath();
    }

    /**
     * Downloads an array of files to a local folder and zips it.
     * 
     * @param proxy
     * @param localDirPath
     * @param remoteFilesPath
     * @param packName
     * @return
     * @throws Exception 
     */
    public static String downloadFiles(String proxy, String localDirPath,
            String[] remoteFilesPath, String packName) throws Exception {

        List<String> downloadedFiles = new ArrayList<String>();
        List<String> errorFiles = new ArrayList<String>();

        for (String remoteFilePath : remoteFilesPath) {
            try {
                String fileName = new File(remoteFilePath).getName();
                String destPath = downloadFile(proxy, localDirPath, fileName, remoteFilePath);
                downloadedFiles.add(destPath);
            } catch (Exception ex) {
                errorFiles.add(remoteFilePath);
                logger.error(ex);
            }
        }

        if (!errorFiles.isEmpty()) {
            downloadedFiles.add(createErrorFile(errorFiles, localDirPath));
        }

        File file = new File(localDirPath);
        String zipName = file.getParent() + "/" + packName + ".zip";

        FolderZipper.zipListOfData(downloadedFiles, zipName);
        FileUtils.deleteDirectory(file);

        return zipName;
    }

    /**
     * Downloads a remote folder to a local folder.
     * 
     * @param proxy
     * @param localDirPath
     * @param remoteDirPath
     * @return
     * @throws Exception 
     */
    public static String downloadFolder(String proxy, String localDirPath,
            String remoteDirPath) throws Exception {

        File localDir = new File(localDirPath);
        if (!localDir.exists()) {
            localDir.mkdirs();
        }

        VletOperations.setProxy(proxy);
        VFSClient vfsClient = Configuration.getInstance().getVfsClient();
        VRL vrlRemote = new VRL("lfn://" + Configuration.LFC_HOST + remoteDirPath);
        VDir remoteDir = vfsClient.getDir(vrlRemote);

        VRL vrlDest = new VRL("file://" + localDir.getAbsolutePath());
        VDir destDir = vfsClient.getDir(vrlDest);

        if (Configuration.getInstance().useLcgCommands()) {
            VFSNode[] nodes = vfsClient.list(vrlRemote);
            List<String> errorFiles = new ArrayList<String>();
            for (VFSNode node : nodes) {
                try {
                    if (node.isDir()) {
                        downloadFolder(proxy,
                                localDirPath + "/" + node.getBasename(),
                                remoteDirPath + "/" + node.getBasename());
                    } else {
                        downloadFile(proxy,
                                localDirPath, node.getBasename(),
                                remoteDirPath + "/" + node.getBasename());
                    }
                } catch (Exception ex) {
                    errorFiles.add(remoteDirPath + "/" + node.getBasename());
                    logger.error("Unable to download '" + remoteDir + "/"
                            + node.getBasename() + "'");
                }
            }
            if (!errorFiles.isEmpty()) {
                createErrorFile(errorFiles, localDirPath);
            }

        } else {
            VletOperations.downloadFolder(proxy, vrlRemote, vrlDest,
                    remoteDir, destDir);
        }
        FolderZipper.zipFolder(destDir.getPath(), destDir.getPath() + ".zip");
        FileUtils.deleteDirectory(new File(destDir.getPath()));
        return destDir.getPath() + ".zip";
    }

    /**
     * 
     * @param errorFiles
     * @param localDirPath
     * @return
     * @throws Exception 
     */
    private static String createErrorFile(List<String> errorFiles,
            String localDirPath) throws Exception {

        String errorFileName = localDirPath + "/README.txt";
        FileWriter fstream = new FileWriter(errorFileName);
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("Unfortunately the following files couldn't be downloaded:\n\n");
        for (String errorFile : errorFiles) {
            out.write(errorFile + "\n");
        }
        out.close();
        return errorFileName;
    }

    /**
     * Replicates a file to the list of preferred SEs.
     * 
     * @param proxy
     * @param sourcePath
     * @throws Exception 
     */
    public static void replicateFile(String proxy, String sourcePath) throws Exception {

        boolean replicated = false;
        if (Configuration.getInstance().useLcgCommands()) {
            replicated = LCGOperations.replicateFile(proxy, sourcePath);
        }
        if (!replicated) {
            VletOperations.replicateFile(proxy, sourcePath);
        }
    }

    /**
     * Deletes a file/directory.
     * 
     * @param proxy
     * @param path
     * @throws Exception 
     */
    public static void delete(String proxy, String path) throws Exception {

        boolean deleted = false;
        try {
            if (Configuration.getInstance().useLcgCommands()) {
                if (LCGOperations.isDir(proxy, path)) {
                    deleted = LCGOperations.deleteFolder(proxy, path);
                } else {
                    deleted = LCGOperations.deleteFile(proxy, path);
                }
            }
        } catch (Exception ex) {
            logger.warn("Failed to perform delete from LCG command.");
        }
        if (!deleted) {
            if (VletOperations.isDir(proxy, path)) {
                VletOperations.deleteFolder(proxy, path);
            } else {
                VletOperations.deleteFile(proxy, path);
            }
        }
    }

    /**
     * 
     * @param proxy
     * @param vfsClient
     * @param path
     * @return
     * @throws Exception 
     */
    public static List<String> listFilesAndFolders(String proxy, String path)
            throws Exception {

        try {
            if (Configuration.getInstance().useLcgCommands()) {
                return LCGOperations.listFilesAndFolders(proxy, path);
            }
        } catch (Exception ex) {
            logger.warn("Failed to perform download from LCG command.");
        }
        return VletOperations.listFilesAndFolders(proxy, path);
    }

    /**
     * Adds a file to the cache.
     * 
     * @param cacheName
     * @param sourcePath
     * @param remoteFilePath
     * @throws Exception 
     */
    public static void addToCache(String cacheName, String sourcePath,
            String remoteFilePath) throws Exception {

        CacheFileDAO cacheFileDAO = DAOFactory.getDAOFactory().getCacheFileDAO();
        long sourceSize = new File(sourcePath).length();

        if ((double) sourceSize <= Configuration.getInstance().getCacheFilesMaxSize()) {

            if (cacheName == null) {
                cacheName = Configuration.getInstance().getCacheFilesPath()
                        + "/" + System.nanoTime() + "-file";
                logger.info("Adding file \"" + remoteFilePath + "\" to cache.");

            } else {
                cacheFileDAO.delete(remoteFilePath);
                new File(cacheName).delete();
                logger.info("Updating file \"" + remoteFilePath + "\" in the cache.");
            }

            if (Configuration.getInstance().getCacheFilesMaxSize()
                    - cacheFileDAO.getTotalUsedSpace() <= sourceSize) {

                List<String> deletedEntries = cacheFileDAO.delete(sourceSize);

                for (String name : deletedEntries) {
                    new File(name).delete();
                }
            }
            FileUtils.copyFile(new File(sourcePath), new File(cacheName));
            cacheFileDAO.add(new CachedFile(remoteFilePath, cacheName, sourceSize));

        } else {
            logger.warn("Unable to add file \"" + remoteFilePath
                    + "\" to the cache. File is bigger than cache size.");
        }
    }
    
    /**
     * Verify if a path exists.
     * 
     * @param proxy
     * @param path
     * @return
     * @throws Exception 
     */
    public static boolean exist(String proxy, String path) throws Exception {
        
        try {
            if (Configuration.getInstance().useLcgCommands()) {
                return LCGOperations.exists(proxy, path);
            }
        } catch (Exception ex) {
            logger.warn("Failed to verify existence from LCG command.");
        }
        return VletOperations.exist(proxy, path);
    }
}
