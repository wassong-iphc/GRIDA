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
import fr.insalyon.creatis.grida.common.bean.CachedFile;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Rafael Silva
 */
public class GRIDACacheClient extends AbstractGRIDAClient {

    /**
     * Creates an instance of the GRIDA Cache Client.
     *
     * @param host GRIDA server host
     * @param port GRIDA server port
     * @param proxyPath Path of the user's proxy file
     */
    public GRIDACacheClient(String host, int port, String proxyPath) {

        super(host, port, proxyPath);
    }

    /**
     * Gets a list of all cached files.
     *
     * @return List of all cached files
     * @throws GRIDAClientException
     */
    public List<CachedFile> getCachedFiles() throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.CACHE_LIST_FILES + Constants.MSG_SEP_1
                    + proxyPath);
            communication.sendEndOfMessage();
            
            String cachedFiles = communication.getMessage();
            communication.close();

            List<CachedFile> cachedFilesList = new ArrayList<CachedFile>();

            if (!cachedFiles.isEmpty()) {
                for (String data : cachedFiles.split(Constants.MSG_SEP_1)) {
                    String[] cachedFile = data.split(Constants.MSG_SEP_2);
                    cachedFilesList.add(new CachedFile(
                            cachedFile[0], cachedFile[1],
                            Double.valueOf(cachedFile[2]), Integer.valueOf(cachedFile[3]),
                            new Date(Long.valueOf(cachedFile[4]))));
                }
            }

            return cachedFilesList;

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }

    /**
     * Deletes a cached file.
     *
     * @param path Grid path
     * @throws GRIDAClientException
     */
    public void deleteCachedFile(String path) throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(
                    ExecutorConstants.CACHE_DELETE_FILE + Constants.MSG_SEP_1
                    + proxyPath + Constants.MSG_SEP_1 + path);
            communication.sendEndOfMessage();

            communication.getMessage();
            communication.close();

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }
}
