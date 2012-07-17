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
package fr.insalyon.creatis.grida.server.execution;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Rafael Ferreira da Silva
 */
public class PoolProcessManager {

    private static PoolProcessManager instance;
    private volatile Map<String, Process> processMap;

    public synchronized static PoolProcessManager getInstance() {

        if (instance == null) {
            instance = new PoolProcessManager();
        }
        return instance;
    }

    private PoolProcessManager() {

        this.processMap = new HashMap<String, Process>();
    }

    /**
     * Adds a process to the manager.
     *
     * @param operationID Operation identification
     * @param process Process object
     */
    public void addProcess(String operationID, Process process) {

        if (operationID != null) {
            this.processMap.put(operationID, process);
        }
    }

    /**
     * Removes a process from the manager.
     *
     * @param operationID Operation identification
     */
    public void removeProcess(String operationID) {

        if (operationID != null && processMap.containsKey(operationID)) {
            this.processMap.remove(operationID);
        }
    }

    /**
     * Destroys a process and removes it from the manager.
     *
     * @param operationID
     */
    public void destroyProcess(String operationID) {

        if (operationID != null && processMap.containsKey(operationID)) {
            processMap.get(operationID).destroy();
            removeProcess(operationID);
        }
    }
}
