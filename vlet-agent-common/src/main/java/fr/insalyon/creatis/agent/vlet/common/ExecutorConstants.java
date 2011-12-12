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
package fr.insalyon.creatis.agent.vlet.common;

/**
 *
 * @author Rafael Silva
 */
public class ExecutorConstants {

    // Regular
    public static final String REG_GET_REMOTE_FILE = "GetRemoteFile";
    public static final String REG_GET_REMOTE_FOLDER = "GetRemoteFolder";
    public static final String REG_LIST_FILES_AND_FOLDERS = "ListFilesAndFolders";
    public static final String REG_GET_MODIFICATION_DATE = "GetModificationDate";
    public static final String REG_UPLOAD_FILE = "UploadFile";
    public static final String REG_UPLOAD_FILE_TO_SES = "UploadFileToSEs";
    public static final String REG_REPLICATE_PREFERRED_SES = "ReplicatePreferredSEs";
    public static final String REG_DELETE = "Delete";
    public static final String REG_CREATE_DIRECTORY = "CreateDirectory";
    public static final String REG_RENAME = "Rename";
    public static final String REG_ALL_CACHED_FILES = "AllCachedFiles";
    public static final String REG_DELETE_CACHED_FILE = "DeleteCachedFile";
    public static final String REG_EXIST = "Exist";
    // Pool
    public static final String POOL_ADD_OPERATION = "PoolAddOperation";
    public static final String POOL_OPERATION_BY_ID = "PoolOperationById";
    public static final String POOL_OPERATIONS_BY_USER = "PoolOperationsByUser";
    public static final String POOL_REMOVE_OPERATION_BY_ID = "PoolRemoveOperationById";
    public static final String POOL_REMOVE_OPERATIONS_BY_USER = "PoolRemoveOperationsByUser";
    public static final String POOL_ALL_OPERATIONS = "PoolAllOperations";
    public static final String POOL_CLEAR_DELETE_OPERATIONS = "ClearDeleteOperations";
}
