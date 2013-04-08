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
package fr.insalyon.creatis.grida.server.operation;

import fr.insalyon.creatis.grida.common.bean.GridData;
import fr.insalyon.creatis.grida.server.Configuration;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import nl.uva.vlet.exception.VlException;
import nl.uva.vlet.tasks.DefaultTaskMonitor;
import nl.uva.vlet.vfs.VDir;
import nl.uva.vlet.vfs.VFSClient;
import nl.uva.vlet.vfs.VFSNode;
import nl.uva.vlet.vfs.VFile;
import nl.uva.vlet.vfs.lfc.LFCFileSystem;
import nl.uva.vlet.vrl.VRL;
import nl.uva.vlet.vrs.VRSContext;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Ferreira da Silva
 */
public class VletOperations {

    public final static Logger logger = Logger.getLogger(VletOperations.class);
    private static VRSContext vrsContext = Configuration.getInstance().getVrsContext();
    private static VFSClient vfsClient = Configuration.getInstance().getVfsClient();

    public static void setProxy(String proxy) {

        logger.info("[VLET] Setting Proxy to: " + proxy);
        vrsContext.setProperty("grid.proxy.location", proxy);
        vrsContext.setGridProxy(null);
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    public static long getModificationDate(String proxy, String path) throws OperationException {

        try {
            setProxy(proxy);
            logger.info("[VLET] Getting modification date for: " + path);
            VRL vrl = new VRL("lfn://" + Configuration.getInstance().getLfcHost() + path);
            VFile vfile = vfsClient.getFile(vrl);
            return vfile.getModificationTime();

        } catch (VlException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
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
    public static List<GridData> listFilesAndFolders(String proxy, String path)
            throws OperationException {

        try {
            setProxy(proxy);
            VRL vrl = new VRL("lfn://" + Configuration.getInstance().getLfcHost() + "/" + path);
            logger.info("[VLET] Listing Files and Folders of: " + path);
            VFSNode[] nodes = vfsClient.list(vrl);

            List<GridData> data = new ArrayList<GridData>();
            for (VFSNode node : nodes) {

                if (node.isDir()) {
                    data.add(new GridData(node.getBasename(), GridData.Type.Folder,
                            node.getAttribute("permissionsString").getStringValue()));
                } else {
                    data.add(new GridData(node.getBasename(), GridData.Type.File,
                            node.getAttribute("length").getLongValue(),
                            node.getAttribute("modificationTime").getStringValue(),
                            node.getAttribute("replicaSEHosts").getStringValue(),
                            node.getAttribute("permissionsString").getStringValue()));
                }
            }
            return data;

        } catch (VlException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param localDir
     * @param fileName
     * @param remoteFilePath
     * @return
     * @throws OperationException
     */
    public static String downloadFile(String proxy, File localDir, String fileName,
            String remoteFilePath) throws OperationException {

        try {
            setProxy(proxy);
            logger.info("[VLET] Downloading: " + remoteFilePath + " - To: " + localDir.getAbsolutePath());
            VFile remoteFile = vfsClient.getFile(new VRL("lfn://"
                    + Configuration.getInstance().getLfcHost() + remoteFilePath));
            VDir destDir = vfsClient.getDir(new VRL("file://" + localDir.getAbsolutePath()));

            return remoteFile.copyTo(destDir).getPath();

        } catch (VlException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
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
            String remoteDir) throws OperationException {

        try {
            setProxy(proxy);
            VRL vrlLocal = new VRL("file://" + localFilePath);
            VFile localFile = vfsClient.getFile(vrlLocal);

            VRL vrlDest = new VRL("lfn://" + Configuration.getInstance().getLfcHost() + remoteDir);
            VDir destDir = vfsClient.getDir(vrlDest);

            logger.info("[VLET] Uploading file: " + vrlLocal.toString() + " - To: " + vrlDest.toString());
            VFile destFile = localFile.copyTo(destDir);

            FileUtils.deleteQuietly(new File(localFilePath));
            return destFile.getPath();

        } catch (VlException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param sourcePath
     * @throws Exception
     */
    public static void replicateFile(String proxy, String sourcePath)
            throws OperationException {

        try {
            setProxy(proxy);
            logger.info("[VLET] Replicating file: " + sourcePath);
            VRL source = new VRL("lfn://" + Configuration.getInstance().getLfcHost() + sourcePath);
            LFCFileSystem server = new LFCFileSystem(vrsContext, null, source);
            VRL vrls[] = new VRL[]{source};
            server.replicateToPreferred(new DefaultTaskMonitor(), vrls);

        } catch (VlException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    public static boolean isDir(String proxy, String path) throws OperationException {

        try {
            setProxy(proxy);
            VRL vrl = new VRL("lfn://" + Configuration.getInstance().getLfcHost() + path);
            VFSNode node = vfsClient.getVFSNode(vrl);

            return node.isDir();

        } catch (VlException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @throws OperationException
     */
    public static void deleteFolder(String proxy, String path) throws OperationException {

        try {
            setProxy(proxy);
            VRL vrl = new VRL("lfn://" + Configuration.getInstance().getLfcHost() + path);
            VDir vdir = vfsClient.getDir(vrl);
            logger.info("[VLET] Deleting folder: " + vrl.toString());
            if (!vdir.delete(true)) {
                logger.error("Unable to delete folder '" + vrl.toString() + "'");
                throw new OperationException("Unable to delete folder '" + vrl.toString() + "'");
            }
        } catch (VlException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @throws OperationException
     */
    public static void deleteFile(String proxy, String path) throws OperationException {

        try {
            setProxy(proxy);
            VRL vrl = new VRL("lfn://" + Configuration.getInstance().getLfcHost() + path);
            VFile vfile = vfsClient.getFile(vrl);
            logger.info("[VLET] Deleting file: " + vrl.toString());
            if (!vfile.delete()) {
                logger.error("Unable to delete file '" + vrl.toString() + "'");
                throw new OperationException("Unable to delete file '" + vrl.toString() + "'");
            }
        } catch (VlException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @throws OperationException
     */
    public static void createFolder(String proxy, String path) throws OperationException {

        try {
            setProxy(proxy);
            VRL vrl = new VRL("lfn://" + Configuration.getInstance().getLfcHost() + path);
            logger.info("[VLET] Creating folder: " + path);
            if (!vfsClient.existsDir(vrl)) {
                VDir vdir = vfsClient.createDir(vrl, false);
                if (vdir == null) {
                    logger.error("Unable to create folder: " + vrl.toString());
                    throw new OperationException("Unable to create folder '" + vrl.toString() + "'");
                }
            } else {
                logger.warn("Folder '" + vrl.toString() + "' already exists.");
            }
        } catch (VlException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param oldPath
     * @param newPath
     * @throws OperationException
     */
    public static void rename(String proxy, String oldPath, String newPath) throws OperationException {

        try {
            setProxy(proxy);
            logger.info("[VLET] Renaming '" + oldPath + "' to '" + newPath + "'");
            VRL vrl = new VRL("lfn://" + Configuration.getInstance().getLfcHost() + "/" + oldPath);

            if (exist(proxy, newPath)) {
                logger.warn("[VLET] File " + newPath + " already exists. Trying with new name.");
                newPath += new SimpleDateFormat("-HH-mm-ss-dd-MM-yyyy").format(new Date());
            }
            vfsClient.rename(vrl, newPath);

        } catch (VlException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
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
     * @param path
     * @return
     * @throws OperationException
     */
    public static boolean exist(String proxy, String path) throws OperationException {

        try {
            setProxy(proxy);
            VRL vrl = new VRL("lfn://" + Configuration.getInstance().getLfcHost() + "/" + path);
            vfsClient.getVFSNode(vrl);

        } catch (VlException ex) {
            if (ex.getMessage().contains("Error while performing:LINKSTAT")) {
                return false;
            }
            throw new OperationException(ex);
        }
        return true;
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    public static long getDataSize(String proxy, String path) throws OperationException {

        try {
            setProxy(proxy);
            VRL vrl = new VRL("lfn://" + Configuration.getInstance().getLfcHost() + "/" + path);
            return vfsClient.getFile(vrl).getLength();

        } catch (VlException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    public static void setComment(String proxy, String lfn, String comment) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
