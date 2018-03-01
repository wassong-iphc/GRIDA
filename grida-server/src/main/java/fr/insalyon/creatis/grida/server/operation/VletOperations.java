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
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Ferreira da Silva
 */
public class VletOperations {

    public final static Logger logger = Logger.getLogger(VletOperations.class);

    public static void setProxy(String proxy) {
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    public static long getModificationDate(String proxy, String path) throws OperationException {
        throw new OperationException("Obsolete");
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
        throw new OperationException("Obsolete");
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
        throw new OperationException("Obsolete");
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
        throw new OperationException("Obsolete");
    }

    /**
     *
     * @param proxy
     * @param sourcePath
     * @throws Exception
     */
    public static void replicateFile(String proxy, String sourcePath)
            throws OperationException {
        throw new OperationException("Obsolete");
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    public static boolean isDir(String proxy, String path) throws OperationException {
        throw new OperationException("Obsolete");
    }

    /**
     *
     * @param proxy
     * @param path
     * @throws OperationException
     */
    public static void deleteFolder(String proxy, String path) throws OperationException {
        throw new OperationException("Obsolete");
    }

    /**
     *
     * @param proxy
     * @param path
     * @throws OperationException
     */
    public static void deleteFile(String proxy, String path) throws OperationException {
        throw new OperationException("Obsolete");
    }

    /**
     *
     * @param proxy
     * @param path
     * @throws OperationException
     */
    public static void createFolder(String proxy, String path) throws OperationException {
        throw new OperationException("Obsolete");
    }

    /**
     *
     * @param proxy
     * @param oldPath
     * @param newPath
     * @throws OperationException
     */
    public static void rename(String proxy, String oldPath, String newPath) throws OperationException {
        throw new OperationException("Obsolete");
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    public static boolean exist(String proxy, String path) throws OperationException {
        throw new OperationException("Obsolete");
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    public static long getDataSize(String proxy, String path) throws OperationException {
        throw new OperationException("Obsolete");
    }

    public static void setComment(String proxy, String lfn, String comment) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
