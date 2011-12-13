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
package fr.insalyon.creatis.grida.server;

import fr.insalyon.creatis.grida.common.Communication;
import fr.insalyon.creatis.grida.server.execution.Executor;
import fr.insalyon.creatis.grida.server.execution.PoolDelete;
import fr.insalyon.creatis.grida.server.execution.PoolDownload;
import fr.insalyon.creatis.grida.server.execution.PoolReplicate;
import fr.insalyon.creatis.grida.server.execution.PoolUpload;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * 
 * @author Rafael Silva
 */
public class Server {

    private static final Logger logger = Logger.getLogger(Server.class);

    public static void main(String[] args) {

        try {
            PropertyConfigurator.configure(Server.class.getClassLoader().getResource("gridaLog4j.properties"));

            Configuration.getInstance();
            logger.info("\nStarting GRIDA Server on port " + Configuration.getInstance().getPort());
            
            // Pools
            PoolDownload.getInstance();
            PoolUpload.getInstance();
            PoolDelete.getInstance();
            PoolReplicate.getInstance();

            // Socket
            ServerSocket serverSocket = new ServerSocket(
                    Configuration.getInstance().getPort(), 50, InetAddress.getLocalHost());

            while (true) {
                Socket socket = serverSocket.accept();
                Communication communication = new Communication(socket);
                new Executor(communication).start();
            }

        } catch (IOException ex) {
            logger.error(ex.getMessage());
        }
    }
}
