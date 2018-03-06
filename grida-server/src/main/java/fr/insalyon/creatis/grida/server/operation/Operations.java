/* Copyright CNRS-CREATIS
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
import java.util.List;

public interface Operations {
    long getModificationDate(String proxy, String path)
        throws OperationException;

    List<GridData> listFilesAndFolders(
        String proxy, String path, boolean listComment /* only in lcg */)
        throws OperationException;

    String downloadFile(
        String operationID,
        String proxy,
        String localDirPath,
        String fileName,
        String remoteFilePath) throws OperationException;

    /** Upload the file to the first SE where it works. If it doesn't work on a
     *  SE, try the next one in the list that is configured.
     */
    String uploadFile(
        String operationID,
        String proxy,
        String localFilePath,
        String remoteDir) throws OperationException;

    void replicateFile(String proxy, String sourcePath)
        throws OperationException;

    boolean isDir(String proxy, String path) throws OperationException;

    void deleteFolder(String proxy, String path) throws OperationException;

    void deleteFile(String proxy, String path) throws OperationException;

    void createFolder(String proxy, String path) throws OperationException;

    void rename(String proxy, String oldPath, String newPath)
        throws OperationException;

    boolean exists(String proxy, String path) throws OperationException;

    /** Gets the size of a file or a directory. */
    long getDataSize(String proxy, String path) throws OperationException;

    void setComment(String proxy, String lfn, String comment)
        throws OperationException;
}
