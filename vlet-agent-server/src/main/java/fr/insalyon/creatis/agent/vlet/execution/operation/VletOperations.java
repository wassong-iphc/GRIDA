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
import fr.insalyon.creatis.agent.vlet.common.bean.GridData;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import nl.uva.vlet.tasks.DefaultTaskMonitor;
import nl.uva.vlet.vfs.VDir;
import nl.uva.vlet.vfs.VFSClient;
import nl.uva.vlet.vfs.VFSNode;
import nl.uva.vlet.vfs.VFile;
import nl.uva.vlet.vfs.lfc.LFCFileSystem;
import nl.uva.vlet.vrl.VRL;
import nl.uva.vlet.vrs.VRSContext;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class VletOperations {

    public final static Logger logger = Logger.getLogger(VletOperations.class);
    private static VRSContext vrsContext = Configuration.getInstance().getVrsContext();
    private static VFSClient vfsClient = Configuration.getInstance().getVfsClient();

    public static void setProxy(String proxy) {
        logger.info("Setting Proxy to: " + proxy);
        vrsContext.setProperty("grid.proxy.location", proxy);
        vrsContext.setGridProxy(null);
    }

    /**
     * 
     * @param proxy
     * @param localFilePath
     * @param remoteDir
     * @return
     * @throws Exception 
     */
    public static String uploadFile(String proxy, String localFilePath,
            String remoteDir) throws Exception {

        setProxy(proxy);
        VRL vrlLocal = new VRL("file://" + localFilePath);
        VFile localFile = vfsClient.getFile(vrlLocal);

        VRL vrlDest = new VRL("lfn://" + Configuration.LFC_HOST + remoteDir);
        VDir destDir = vfsClient.getDir(vrlDest);

        logger.info("Uploading file: " + vrlLocal.toString() + " - To: " + vrlDest.toString());
        VFile destFile = localFile.copyTo(destDir);

        localFile.delete();

        return destFile.getPath();
    }

    /**
     * 
     * @param proxy
     * @param vrlRemote
     * @param vrlDest
     * @param remoteFile
     * @param destFile
     * @param localDir
     * @return
     * @throws Exception 
     */
    public static String downloadFile(String proxy, VRL vrlRemote, VRL vrlDest,
            VFile remoteFile, VFile destFile, File localDir) throws Exception {

        setProxy(proxy);
        logger.info("Downloading File: " + vrlRemote.toString() + " - To: " + vrlDest.toString());
        VDir destDir = vfsClient.getDir(new VRL("file://" + localDir.getAbsolutePath()));
        destFile = remoteFile.copyTo(destDir);

        return destFile.getPath();
    }

    /**
     * 
     * @param logger
     * @param vrlRemote
     * @param vrlDest
     * @param remoteDir
     * @param destDir
     * @return
     * @throws Exception 
     */
    public static String downloadFolder(String proxy, VRL vrlRemote, VRL vrlDest,
            VDir remoteDir, VDir destDir) throws Exception {

        setProxy(proxy);
        logger.info("Downloading Folder: " + vrlRemote.toString() + " - To: " + vrlDest.toString());
        destDir = remoteDir.copyTo(destDir);

        return destDir.getPath();
    }

    /**
     * 
     * @param proxy
     * @param sourcePath
     * @throws Exception 
     */
    public static void replicateFile(String proxy, String sourcePath) throws Exception {

        setProxy(proxy);
        logger.info("Replicating file: " + sourcePath);
        VRL source = new VRL("lfn://" + Configuration.LFC_HOST + sourcePath);
        LFCFileSystem server = new LFCFileSystem(vrsContext, null, source);
        VRL vrls[] = new VRL[]{source};
        server.replicateToPreferred(new DefaultTaskMonitor(), vrls);
    }

    /**
     * 
     * @param proxy
     * @param vrl
     * @throws Exception 
     */
    public static void deleteFile(String proxy, String path) throws Exception {

        setProxy(proxy);
        VRL vrl = new VRL("lfn://" + Configuration.LFC_HOST + path);
        VFile vfile = vfsClient.getFile(vrl);
        logger.info("Deleting file: " + vrl.toString());
        if (!vfile.delete()) {
            throw new Exception("Unable to delete file '" + vrl.toString() + "'");
        }
    }

    /**
     * 
     * @param proxy
     * @param path
     * @throws Exception 
     */
    public static void deleteFolder(String proxy, String path) throws Exception {

        setProxy(proxy);
        VRL vrl = new VRL("lfn://" + Configuration.LFC_HOST + path);
        VDir vdir = vfsClient.getDir(vrl);
        logger.info("Deleting directory: " + vrl.toString());
        if (!vdir.delete(true)) {
            throw new Exception("Unable to delete directory '" + vrl.toString() + "'");
        }
    }

    /**
     * 
     * @param proxy
     * @param path
     * @return
     * @throws Exception 
     * 
     * Attributes:
     * 
     * type, name, scheme, hostname, port, mimeType, isReadable, isWritable, 
     * isHidden, isFile, isDir, nrChilds, length, modificationTime, 
     * parentDirname, isSymbolicLink, permissionsString, symbolicLinkTarget, 
     * groupID, userID, gridUniqueID, unixFileMode, creationTime, accessTime, 
     * lfcFileId, lfcFileClass, lfcULink, lfcStatus, lfcComment, nrOfReplicas, 
     * replicaSEHosts, iconURL, path, location
     */
    public static List<String> listFilesAndFolders(String proxy, String path) throws Exception {

        setProxy(proxy);
        VRL vrl = new VRL("lfn://" + Configuration.LFC_HOST + "/" + path);
        logger.info("Listing Files and Folders of: " + path);
        VFSNode[] nodes = vfsClient.list(vrl);

        String SEP = "--";
        List<String> paths = new ArrayList<String>();
        for (VFSNode node : nodes) {

            String data = "";
            if (node.isDir()) {
                data = node.getBasename() + SEP
                        + GridData.Type.Folder.name() + SEP
                        + node.getAttribute("permissionsString");
            } else {
                data = node.getBasename() + SEP
                        + GridData.Type.File.name() + SEP
                        + node.getAttribute("length").getStringValue() + SEP
                        + node.getAttribute("modificationTime").getStringValue() + SEP
                        + node.getAttribute("replicaSEHosts") + SEP
                        + node.getAttribute("permissionsString");
            }
            paths.add(data);
        }

        return paths;
    }

    /**
     * 
     * @param proxy
     * @param path
     * @return
     * @throws Exception 
     */
    public static long getModificationDate(String proxy, String path) throws Exception {

        setProxy(proxy);
        VRL vrl = new VRL("lfn://" + Configuration.LFC_HOST + path);
        logger.info("Getting modification date for: " + vrl.toString());
        VFile vfile = vfsClient.getFile(vrl);
        return vfile.getModificationTime();
    }

    /**
     * 
     * @param proxy
     * @param path
     * @throws Exception 
     */
    public static void createFolder(String proxy, String path) throws Exception {

        setProxy(proxy);
        VRL vrl = new VRL("lfn://" + Configuration.LFC_HOST + path);
        logger.info("Creating folder: " + path);
        if (!vfsClient.existsDir(vrl)) {
            VDir vdir = vfsClient.createDir(vrl, false);
            if (vdir == null) {
                logger.error("Unable to create folder: " + vrl.toString());
                throw new Exception("Unable to create folder '" + vrl.toString() + "'");
            }
        } else {
            logger.warn("Folder '" + vrl.toString() + "' already exists.");
        }
    }

    /**
     * 
     * @param proxy
     * @param path
     * @return
     * @throws Exception 
     */
    public static boolean isDir(String proxy, String path) throws Exception {

        setProxy(proxy);
        VFSClient vfsClient = Configuration.getInstance().getVfsClient();
        VRL vrl = new VRL("lfn://" + Configuration.LFC_HOST + path);
        VFSNode node = vfsClient.getVFSNode(vrl);

        return node.isDir();
    }

    /**
     * 
     * @param proxy
     * @param oldName
     * @param newName
     * @throws Exception 
     */
    public static boolean rename(String proxy, String oldName, String newName) throws Exception {

        setProxy(proxy);
        logger.info("Renaming '" + oldName + "' to '" + newName + "'");
        VRL vrl = new VRL("lfn://" + Configuration.LFC_HOST + "/" + oldName);
        return vfsClient.rename(vrl, newName);
    }

    /**
     * 
     * @param proxy
     * @param path
     * @return
     * @throws Exception 
     */
    public static boolean exist(String proxy, String path) throws Exception {

        try {
            setProxy(proxy);
            VRL vrl = new VRL("lfn://" + Configuration.LFC_HOST + "/" + path);
            vfsClient.getVFSNode(vrl);
        } catch (Exception ex) {
            if (ex.getMessage().equals("Error while performing:LINKSTAT")) {
                return false;
            }
            throw ex;
        }
        return true;
    }
}
