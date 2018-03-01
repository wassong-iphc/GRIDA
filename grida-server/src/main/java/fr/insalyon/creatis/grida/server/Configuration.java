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
package fr.insalyon.creatis.grida.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Ferreira da Silva
 */
public class Configuration {

    private static final Logger logger = Logger.getLogger(Configuration.class);
    private static Configuration instance;
    private static final String confFile = "grida-server.conf";
    // General
    private int port;
    private int maxRetryCount;
    private double minAvailableDiskSpace;
    private String lfcHost;
    private String vo;
    private String bdiiHost;
    private String bdiiPort;
    private List<String> preferredSEsList;
    private List<String> failoverServers;
    private boolean isLcgCommandsAvailable = false;
    // Cache
    private int cacheListMaxEntries;
    private int cacheListMaxHours;
    private double cacheFilesMaxSize;
    private String cacheFilesPath;
    // Pool
    private int maxHistory;
    private int maxSimultaneousDownloads;
    private int maxSimultaneousUploads;
    private int maxSimultaneousDeletes;
    private int maxSimultaneousReplications;

    public synchronized static Configuration getInstance() {

        if (instance == null) {
            instance = new Configuration();
        }
        return instance;
    }

    private Configuration() {

        loadConfigurationFile();
        createCachePath();
        configureLcgCommands();
        if (!isLcgCommandsAvailable) {
            System.exit(1);
        }
    }

    private void loadConfigurationFile() {

        try {
            logger.info("Loading configuration file.");
            PropertiesConfiguration config = new PropertiesConfiguration(new File(confFile));

            port = config.getInt(Constants.LAB_AGENT_PORT, 9006);
            maxRetryCount = config.getInt(Constants.LAB_AGENT_RETRYCOUNT, 5);
            minAvailableDiskSpace = config.getDouble(Constants.LAB_AGENT_MIN_AVAILABLE_DISKSPACE, 0.1);
            lfcHost = config.getString(Constants.LAB_LFC_HOST, "lfc-biomed.in2p3.fr");
            vo = config.getString(Constants.LAB_VO, "biomed");
            bdiiHost = config.getString(Constants.LAB_BDII_HOST, "cclcgtopbdii02.in2p3.fr");
            bdiiPort = config.getString(Constants.LAB_BDII_PORT, "2170");
            preferredSEsList = config.getList(Constants.LAB_LFC_PREFERRED_SES, new ArrayList<String>());
            cacheListMaxEntries = config.getInt(Constants.LAB_CACHE_MAX_ENTRIES, 30);
            cacheListMaxHours = config.getInt(Constants.LAB_CACHE_MAX_HOURS, 12);
            cacheFilesMaxSize = config.getDouble(Constants.LAB_CACHE_MAX_SIZE, 100) * 1024 * 1024;
            cacheFilesPath = config.getString(Constants.LAB_CACHE_PATH, ".cache");
            failoverServers = config.getList(Constants.LAB_FAILOVER_SERVERS, new ArrayList<String>());
            maxSimultaneousDownloads = config.getInt(Constants.LAB_POOL_MAX_DOWNLOAD, 10);
            maxSimultaneousUploads = config.getInt(Constants.LAB_POOL_MAX_UPLOAD, 10);
            maxSimultaneousDeletes = config.getInt(Constants.LAB_POOL_MAX_DELETE, 5);
            maxSimultaneousReplications = config.getInt(Constants.LAB_POOL_MAX_REPLICATION, 5);
            maxHistory = config.getInt(Constants.LAB_POOL_MAX_HISTORY, 120);

            config.setProperty(Constants.LAB_AGENT_PORT, port);
            config.setProperty(Constants.LAB_AGENT_RETRYCOUNT, maxRetryCount);
            config.setProperty(Constants.LAB_AGENT_MIN_AVAILABLE_DISKSPACE, minAvailableDiskSpace);
            config.setProperty(Constants.LAB_LFC_HOST, lfcHost);
            config.setProperty(Constants.LAB_VO, vo);
            config.setProperty(Constants.LAB_BDII_HOST, bdiiHost);
            config.setProperty(Constants.LAB_BDII_PORT, bdiiPort);
            config.setProperty(Constants.LAB_LFC_PREFERRED_SES, preferredSEsList);
            config.setProperty(Constants.LAB_CACHE_MAX_ENTRIES, cacheListMaxEntries);
            config.setProperty(Constants.LAB_CACHE_MAX_HOURS, cacheListMaxHours);
            config.setProperty(Constants.LAB_CACHE_MAX_SIZE, cacheFilesMaxSize / (1024 * 1024));
            config.setProperty(Constants.LAB_CACHE_PATH, cacheFilesPath);
            config.setProperty(Constants.LAB_FAILOVER_SERVERS, failoverServers);
            config.setProperty(Constants.LAB_POOL_MAX_DOWNLOAD, maxSimultaneousDownloads);
            config.setProperty(Constants.LAB_POOL_MAX_UPLOAD, maxSimultaneousUploads);
            config.setProperty(Constants.LAB_POOL_MAX_DELETE, maxSimultaneousDeletes);
            config.setProperty(Constants.LAB_POOL_MAX_REPLICATION, maxSimultaneousReplications);
            config.setProperty(Constants.LAB_POOL_MAX_HISTORY, maxHistory);

            config.save();

        } catch (ConfigurationException ex) {
            logger.error(ex);
        }
    }

    private void createCachePath() {

        File cacheDir = new File(cacheFilesPath);
        if (!cacheDir.exists()) {
            if (!cacheDir.mkdirs()) {
                logger.error("Unable to create cache folder at: " + cacheDir.getAbsolutePath());
                logger.error("Stopping GRIDA Server.");
                System.exit(1);
            }
        }
    }

    private void configureLcgCommands() {

        try {
            ProcessBuilder builder = new ProcessBuilder("which", "lcg-cr");
            builder.redirectErrorStream(true);
            Process process = builder.start();
            process.waitFor();

            if (process.exitValue() == 0) {
                isLcgCommandsAvailable = true;
            } else {
                logger.warn("LCG Commands unavailable.");
                return;
            }

            builder = new ProcessBuilder("which", "lcg-cp");
            builder.redirectErrorStream(true);
            process = builder.start();
            process.waitFor();

            if (process.exitValue() != 0) {
                isLcgCommandsAvailable = false;
                logger.warn("LCG Commands unavailable.");
            } else {
                logger.info("LCG Commands available.");
            }

        } catch (InterruptedException ex) {
            logger.warn("LCG Commands unavailable.");
            isLcgCommandsAvailable = false;
        } catch (IOException ex) {
            logger.warn("LCG Commands unavailable.");
            isLcgCommandsAvailable = false;
        }
    }

    public String getLfcHost() {
        return lfcHost;
    }

    public int getPort() {
        return port;
    }

    public List<String> getPreferredSEs() {
        return preferredSEsList;
    }

    public void setPreferredSEs() {

        StringBuilder sb = new StringBuilder();
        for (String se : preferredSEsList) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(se);
        }
    }

    public void setPreferredSEs(String list) {
        // This was only used for vlet.  Should it be removed ????
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public double getMinAvailableDiskSpace() {
        return minAvailableDiskSpace;
    }

    public int getCacheListMaxEntries() {
        return cacheListMaxEntries;
    }

    public int getCacheListMaxHours() {
        return cacheListMaxHours;
    }

    public double getCacheFilesMaxSize() {
        return cacheFilesMaxSize;
    }

    public String getCacheFilesPath() {
        return new File(cacheFilesPath).getAbsolutePath();
    }

    public boolean isLcgCommandsAvailable() {
        return isLcgCommandsAvailable;
    }

    public String getVo() {
        return vo;
    }

    public List<String> getFailoverServers() {
        return failoverServers;
    }

    public int getMaxSimultaneousDownloads() {
        return maxSimultaneousDownloads;
    }

    public int getMaxSimultaneousUploads() {
        return maxSimultaneousUploads;
    }

    public int getMaxSimultaneousDeletes() {
        return maxSimultaneousDeletes;
    }

    public int getMaxSimultaneousReplications() {
        return maxSimultaneousReplications;
    }

    public int getMaxHistory() {
        return maxHistory;
    }
}
