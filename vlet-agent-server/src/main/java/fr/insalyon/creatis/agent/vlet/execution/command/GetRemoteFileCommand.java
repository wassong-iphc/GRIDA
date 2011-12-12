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
import fr.insalyon.creatis.agent.vlet.dao.CacheFileDAO;
import fr.insalyon.creatis.agent.vlet.dao.DAOFactory;
import fr.insalyon.creatis.agent.vlet.execution.operation.Operations;
import fr.insalyon.creatis.agent.vlet.execution.operation.LCGOperations;
import fr.insalyon.creatis.agent.vlet.execution.operation.VletOperations;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class GetRemoteFileCommand extends Command {

    private static final Logger logger = Logger.getLogger(GetRemoteFileCommand.class);
    private String remoteFilePath;
    private String localDirPath;

    public GetRemoteFileCommand(Communication communication, String proxyFileName, String remoteFilePath, String localDirPath) {
        super(communication, proxyFileName);
        this.remoteFilePath = remoteFilePath;
        this.localDirPath = localDirPath;
    }

    @Override
    public void execute() {

        try {
            CacheFileDAO cacheFileDAO = DAOFactory.getDAOFactory().getCacheFileDAO();
            String cacheName = cacheFileDAO.getName(remoteFilePath);
            String fileName = new File(remoteFilePath).getName();

            if (cacheName == null) {
                String destPath = downloadFile(fileName);
                Operations.addToCache(cacheName, destPath, remoteFilePath);

            } else {
                
                long remoteFileTime = -1;
                long cacheFileTime = new File(cacheName).lastModified();
                
                if (Configuration.getInstance().useLcgCommands()) {
                    remoteFileTime = LCGOperations.getModificationDate(proxyFileName, remoteFilePath);
                } else {
                    remoteFileTime = VletOperations.getModificationDate(proxyFileName, remoteFilePath);
                }
                
                if (remoteFileTime <= cacheFileTime) {
                    String destPath = localDirPath + "/" + fileName;
                    FileUtils.copyFile(new File(cacheName), new File(destPath));
                    logger.info("Copying file \"" + remoteFilePath + "\" from the cache.");
                    communication.sendMessage(destPath);

                } else {
                    String destPath = downloadFile(fileName);
                    Operations.addToCache(cacheName, destPath, remoteFilePath);
                }
                cacheFileDAO.update(remoteFilePath);
            }
        } catch (Exception ex) {
            logException(logger, ex);
            communication.sendErrorMessage(ex.getMessage());
        }
        communication.sendEndOfMessage();
    }

    /**
     * Downloads a file from the grid
     * 
     * @param fileName
     * @return
     * @throws Exception
     */
    private String downloadFile(String fileName) throws Exception {

        String destPath = Operations.downloadFile(proxyFileName, localDirPath, 
                fileName, remoteFilePath);
        communication.sendMessage(destPath);
        return destPath;
    }
}
