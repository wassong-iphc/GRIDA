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
import fr.insalyon.creatis.agent.vlet.Configuration;
import fr.insalyon.creatis.agent.vlet.dao.CacheListDAO;
import fr.insalyon.creatis.agent.vlet.dao.DAOFactory;
import fr.insalyon.creatis.agent.vlet.execution.operation.Operations;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class ListFilesAndFoldersCommand extends Command {

    private static final Logger logger = Logger.getLogger(ListFilesAndFoldersCommand.class);
    private String path;
    private boolean refresh;
    private CacheListDAO cacheListDAO;

    public ListFilesAndFoldersCommand(Communication communication,
            String proxyFileName, String path, String refresh) {
        super(communication, proxyFileName);
        this.path = path;
        this.refresh = Boolean.valueOf(refresh);
        cacheListDAO = DAOFactory.getDAOFactory().getCacheListDAO();
    }

    @Override
    public void execute() {
        try {
            if (refresh) {
                this.getFilesList();

            } else {
                Date lastRefresh = cacheListDAO.getLastRefresh(path);
                if (lastRefresh != null) {

                    Calendar currentCal = Calendar.getInstance();
                    Calendar calLastRefresh = Calendar.getInstance();
                    calLastRefresh.setTime(lastRefresh);
                    calLastRefresh.add(Calendar.HOUR_OF_DAY,
                            Configuration.getInstance().getCacheListMaxHours());

                    if (calLastRefresh.after(currentCal)) {
                        List<String> paths = cacheListDAO.getPathList(path);
                        logger.info("Listing Files and Folders from cache: " + path);
                        for (String p : paths) {
                            communication.sendMessage(p);
                        }
                    } else {
                        logger.info("Cache list expired for path: " + path);
                        this.getFilesList();
                    }
                } else {
                    this.getFilesList();
                }
            }
        } catch (Exception ex) {
            logException(logger, ex);
            communication.sendErrorMessage(ex.getMessage());
        }
        communication.sendEndOfMessage();
    }

    private void getFilesList() throws Exception {
      
        List<String> paths = Operations.listFilesAndFolders(proxyFileName, path);
        
        for (String p : paths) {
            communication.sendMessage(p);
        }
        
        if (cacheListDAO.getNumberOfEntries() == Configuration.getInstance().getCacheListMaxEntries()) {
            cacheListDAO.delete();
        }
        if (cacheListDAO.getPathList(path) == null) {
            cacheListDAO.add(path, paths);
        } else {
            cacheListDAO.update(path, paths);
        }
    }
}
