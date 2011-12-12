/* Copyright CNRS-CREATIS
 *
 * Rafael Silva
 * rafael.silva@creatis.insa-lyon.fr
 * http://www.creatis.insa-lyon.fr/~silva
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
package fr.insalyon.creatis.agent.vlet.client;

import fr.insalyon.creatis.agent.vlet.common.Constants;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 *
 * @author Rafael Silva
 */
public class Communication {

    private Socket socket;
    private BufferedReader in;
    private PrintWriter out;

    /**
     * Connects to a Vlet Agent
     * 
     * @param host Vlet Agent host
     * @param port Vlet Agent port
     * @throws VletAgentClientException
     */
    protected Communication(String host, int port) throws VletAgentClientException {
        try {
            this.socket = new Socket(host, port);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.out = new PrintWriter(socket.getOutputStream(), true);

        } catch (UnknownHostException ex) {
            throw new VletAgentClientException(ex);
        } catch (IOException ex) {
            throw new VletAgentClientException(ex);
        }
    }

    /**
     * Closes the connection with the Vlet Agent
     */
    protected void close() {
        try {
            socket.close();

        } catch (IOException ex) {
            // do nothing
        }
    }

    /**
     * Sends a message to the Vlet Agent
     * 
     * @param message
     * @throws VletAgentClientException
     */
    protected void sendMessage(String message) {
        out.println(message);
        out.flush();
    }

    /**
     * Receives a message from the Vlet Agent
     * 
     * @return Message received
     * @throws VletAgentClientException
     */
    protected String getMessage() throws VletAgentClientException {
        try {
            StringBuilder messageBuilder = new StringBuilder();

            String message = in.readLine();
            if (message == null || message.startsWith(Constants.ERROR)) {
                throw new VletAgentClientException(message);
            }

            while (!message.equals(Constants.END_OF_MESSAGE)) {
                if (!messageBuilder.toString().isEmpty()) {
                    messageBuilder.append(Constants.SEPARATOR);
                }
                messageBuilder.append(message);
                message = in.readLine();
            }

            return messageBuilder.toString();

        } catch (IOException ex) {
            throw new VletAgentClientException(ex);
        }
    }
}
