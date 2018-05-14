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

import fr.insalyon.creatis.grida.server.operation.DiracOperations;
import fr.insalyon.creatis.grida.server.operation.LCGOperations;
import fr.insalyon.creatis.grida.server.operation.Operations;
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

    private String commandsType;
    private String diracBashrc;
    private Operations operations;

    public synchronized static Configuration getInstance() {

        if (instance == null) {
            instance = new Configuration();
        }
        return instance;
    }

    private Configuration() {
        boolean isOneCommandConfigured = false;

        loadConfigurationFile();
        createCachePath();
        switch (commandsType) {
        case "lcg":
        {
            boolean isLcgCommandsAvailable =
                isBinaryAvailable("lcg-cr", null)
                && isBinaryAvailable("lcg-cp", null);
            if (isLcgCommandsAvailable) {
                logger.info("LCG commands available.");
                isOneCommandConfigured = true;
                operations = new LCGOperations();
            } else {
                logger.warn("LCG commands unavailable.");
            }
        }
        break;
        case "dirac":
        {
            boolean exists = new File(diracBashrc).exists();
            if (!exists) {
                logger.warn("Dirac bashrc file does not exist: " + diracBashrc);
            }
            boolean isDiracCommandsAvailable =
                exists
                && isBinaryAvailable("dls", diracBashrc)
                && isBinaryAvailable("dget", diracBashrc);
            if (isDiracCommandsAvailable) {
                logger.info("Dirac commands available.");
                isOneCommandConfigured = true;
                operations = new DiracOperations(diracBashrc);
            } else {
                logger.warn("Dirac commands unavailable.");
            }
        }
        break;
        default:
            logger.error("Unkown command type: " + commandsType +
                         ". Possible values are lcg or dirac.");
            break;
        }
        if (!isOneCommandConfigured) {
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

            commandsType = config.getString(Constants.LAB_COMMANDS_TYPE, "lcg");
            diracBashrc = config.getString(Constants.LAB_DIRAC_BASHRC, "needed_if_commands.type_is_dirac");

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
            config.setProperty(Constants.LAB_COMMANDS_TYPE, commandsType);
            config.setProperty(Constants.LAB_DIRAC_BASHRC, diracBashrc);

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

    private boolean isBinaryAvailable(String name, String envFile) {
        boolean isAvailable = false;
        try {
            String command = envFile == null
                ? "which"
                : "source " + envFile + "; which";

            ProcessBuilder builder = envFile == null
                ? new ProcessBuilder("which", name)
                : new ProcessBuilder(
                    "bash", "-c", "source " + envFile + "; which " + name);
            builder.redirectErrorStream(true);
            Process process = builder.start();
            process.waitFor();

            isAvailable = process.exitValue() == 0;
        } catch (InterruptedException ex) {
            logger.warn(ex);
        } catch (IOException ex) {
            logger.warn(ex);
        }
        return isAvailable;
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

    public Operations getOperations() {
        return operations;
    }
}
