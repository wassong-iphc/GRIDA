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
package fr.insalyon.creatis.agent.vlet.execution.command;

import fr.insalyon.creatis.agent.vlet.Communication;
import fr.insalyon.creatis.agent.vlet.execution.operation.VletOperations;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class RenameCommand extends Command {

    private static final Logger logger = Logger.getLogger(RenameCommand.class);
    private String oldName;
    private String newName;

    public RenameCommand(Communication communication, String proxyFileName,
            String oldName, String newName) {

        super(communication, proxyFileName);
        this.oldName = oldName;
        this.newName = newName;
    }

    @Override
    public void execute() {

        try {
            rename();

        } catch (Exception ex) {

            if (ex.getMessage().contains("File/Directory exists or Directory is not empty")) {
                try {
                    logger.warn("File " + newName + " already exists. Trying with new name.");
                    newName += new SimpleDateFormat("-HH-mm-ss-dd-MM-yyyy").format(new Date());
                    rename();

                } catch (Exception ex1) {
                    logException(logger, ex);
                    communication.sendErrorMessage(ex.getMessage());
                }
            } else {
                logException(logger, ex);
                communication.sendErrorMessage(ex.getMessage());
            }
        }
        communication.sendEndOfMessage();
    }

    /**
     * Renames a file/directory in the LFC catalog.
     * 
     * @throws Exception 
     */
    private void rename() throws Exception {

        logger.info("Renaming '" + oldName + "' to '" + newName + "'");
        if (VletOperations.rename(proxyFileName, oldName, newName)) {
            communication.sendSucessMessage();
        } else {
            communication.sendErrorMessage("Unable to rename '" + oldName + "' to '" + newName + "'");
            logException(logger, new Exception("Unable to rename '" + oldName + "' to '" + newName + "'"));
        }
    }
}
