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
package fr.insalyon.creatis.grida.common;

/**
 *
 * @author Rafael Silva
 */
public class ExecutorConstants {

    // General Commands
    public static final int COM_GET_REMOTE_FILE = 101;
    public static final int COM_GET_REMOTE_FOLDER = 102;
    public static final int COM_LIST_FILES_AND_FOLDERS = 103;
    public static final int COM_GET_MODIFICATION_DATE = 104;
    public static final int COM_UPLOAD_FILE = 105;
    public static final int COM_UPLOAD_FILE_TO_SES = 106;
    public static final int COM_REPLICATE_PREFERRED_SES = 107;
    public static final int COM_DELETE = 108;
    public static final int COM_CREATE_FOLDER = 109;
    public static final int COM_RENAME = 110;
    public static final int COM_EXIST = 111;
    // Cache Commands
    public static final int CACHE_LIST_FILES = 201;
    public static final int CACHE_DELETE_FILE = 202;
    // Pool
    public static final int POOL_ADD_OPERATION = 301;
    public static final int POOL_OPERATION_BY_ID = 302;
    public static final int POOL_OPERATIONS_BY_USER = 303;
    public static final int POOL_REMOVE_OPERATION_BY_ID = 304;
    public static final int POOL_REMOVE_OPERATIONS_BY_USER = 305;
    public static final int POOL_ALL_OPERATIONS = 306;
}
