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
import fr.insalyon.creatis.grida.common.bean.ZombieFile;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Rafael Silva
 */
public class GRIDAZombieClient extends AbstractGRIDAClient {

    /**
     * Creates an instance of the GRIDA Zombie Client.
     *
     * @param host GRIDA server host
     * @param port GRIDA server port
     * @param proxyPath Path of the user's proxy file
     */
    public GRIDAZombieClient(String host, int port, String proxyPath) {

        super(host, port, proxyPath);
    }

    /**
     * Gets a list of all zombie files.
     * 
     * @return List of all zombie files
     * @throws GRIDAClientException 
     */
    public List<ZombieFile> getList() throws GRIDAClientException {

        try {
            Communication communication = getCommunication();
            communication.sendMessage(ExecutorConstants.ZOM_GET 
                    + Constants.MSG_SEP_1 + proxyPath);
            communication.sendEndOfMessage();

            String zombieFiles = communication.getMessage();

            List<ZombieFile> list = new ArrayList<ZombieFile>();

            if (!zombieFiles.isEmpty()) {
                for (String zf : zombieFiles.split(Constants.MSG_SEP_1)) {
                    String[] d = zf.split(Constants.MSG_SEP_2);
                    list.add(new ZombieFile(d[0], new Date(Long.parseLong(d[1]))));
                }
            }
            return list;

        } catch (IOException ex) {
            throw new GRIDAClientException(ex);
        }
    }
}
