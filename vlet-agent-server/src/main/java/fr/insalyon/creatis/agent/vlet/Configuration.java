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
package fr.insalyon.creatis.agent.vlet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import nl.uva.vlet.GlobalConfig;
import nl.uva.vlet.vfs.VFSClient;
import nl.uva.vlet.vrs.VRSContext;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class Configuration {

    private static final Logger logger = Logger.getLogger(Configuration.class);
    private static Configuration instance;
    private static final String confFile = "vlet-agent.conf";
    private static Properties conf;
    public static int PORT = 9006;
    public static String LFC_HOST = "lfc-biomed.in2p3.fr";
    private String vo = "biomed";
    private String bdiiHost = "cclcgtopbdii02.in2p3.fr";
    private String bdiiPort = "2170";
    private String preferredSEsList = "ccsrm02.in2p3.fr";
    private int maxRetryCount = 5;
    private int cacheListMaxEntries = 30;
    private int cacheListMaxHours = 12;
    private double cacheFilesMaxSize = 100 * 1024 * 1024;
    private String cacheFilesPath = ".cache";
    private VRSContext vrsContext;
    private VFSClient vfsClient;
    private boolean useLcgCommands = false;
    private List<String> failoverServers;

    public synchronized static Configuration getInstance() {
        if (instance == null) {
            instance = new Configuration();
        }
        return instance;
    }

    private Configuration() {

        loadConfigurationFile();
        createCachePath();
        configureVlet();
        configureLcgCommands();
    }

    private void loadConfigurationFile() {
        
        try {
            logger.info("Loading configuration file.");
            PropertiesConfiguration config = new PropertiesConfiguration(new File(confFile));

            PORT = config.getInt("agent.port", PORT);
            maxRetryCount = config.getInt("agent.retrycount", maxRetryCount);
            LFC_HOST = config.getString("lfc.host", LFC_HOST);
            vo = config.getString("vo", vo);
            bdiiHost = config.getString("bdii.host", bdiiHost);
            bdiiPort = config.getString("bdii.port", bdiiPort);
            preferredSEsList = config.getString("lfc.preferredSEsList", preferredSEsList);
            cacheListMaxEntries = config.getInt("cache.list.max.entries", cacheListMaxEntries);
            cacheListMaxHours = config.getInt("cache.list.max.hours", cacheListMaxHours);
            cacheFilesMaxSize = config.getDouble("cache.files.max.size", cacheFilesMaxSize / (1024 * 1024)) * 1024 * 1024;
            cacheFilesPath = config.getString("cache.files.path", cacheFilesPath);
            failoverServers = config.getList("failover.servers", new ArrayList<String>());

            config.setProperty("agent.port", PORT);
            config.setProperty("agent.retrycount", maxRetryCount);
            config.setProperty("lfc.host", LFC_HOST);
            config.setProperty("vo", vo);
            config.setProperty("bdii.host", bdiiHost);
            config.setProperty("bdii.port", bdiiPort);
            config.setProperty("lfc.preferredSEsList", preferredSEsList);
            config.setProperty("cache.list.max.entries", cacheListMaxEntries);
            config.setProperty("cache.list.max.hours", cacheListMaxHours);
            config.setProperty("cache.files.max.size", cacheFilesMaxSize / (1024 * 1024));
            config.setProperty("cache.files.path", cacheFilesPath);
            config.setProperty("failover.servers", failoverServers);
            
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
                logger.error("Stopping Vlet Agent.");
                System.exit(1);
            }
        }
    }

    private void configureVlet() {
        // Create vlet properties file
        File vletDir = new File(System.getenv("HOME") + "/.vletrc");
        if (!vletDir.exists()) {
            vletDir.mkdir();
        }
        File vletProp = new File(System.getenv("HOME") + "/.vletrc/vletrc.prop");
        if (!vletProp.exists()) {
            try {
                vletProp.createNewFile();
            } catch (IOException ex) {
                logger.error("Unable to create vlet properties file: " + ex.getMessage());
                logger.error("Stopping Vlet Agent.");
                System.exit(1);
            }
        }

        // Configuring vlet
        GlobalConfig.setSystemProperty("bdii.hostname", bdiiHost);
        GlobalConfig.setSystemProperty("bdii.port", bdiiPort);
        if (!preferredSEsList.equals("")) {
            setPreferredSEs();
            GlobalConfig.setSystemProperty("lfc.replicaCreationMode", "PreferredRandom");
            GlobalConfig.setSystemProperty("lfc.replicaSelectionMode", "AllRandom");
            GlobalConfig.setSystemProperty("lfc.replicaNrOfTries", "5");
        }
        vrsContext = new VRSContext();
        vfsClient = new VFSClient(vrsContext);
    }

    private void configureLcgCommands() {
        try {
            ProcessBuilder builder = new ProcessBuilder("which", "lcg-cr");
            builder.redirectErrorStream(true);
            Process process = builder.start();
            process.waitFor();

            if (process.exitValue() == 0) {
                useLcgCommands = true;
            } else {
                logger.warn("LCG Commands unavailable.");
                return;
            }

            builder = new ProcessBuilder("which", "lcg-cp");
            builder.redirectErrorStream(true);
            process = builder.start();
            process.waitFor();

            if (process.exitValue() != 0) {
                useLcgCommands = false;
                logger.warn("LCG Commands unavailable.");
            } else {
                logger.info("LCG Commands available.");
            }

        } catch (InterruptedException ex) {
            logger.warn("LCG Commands unavailable.");
            useLcgCommands = false;
        } catch (IOException ex) {
            logger.warn("LCG Commands unavailable.");
            useLcgCommands = false;
        }
    }

    public VFSClient getVfsClient() {
        return vfsClient;
    }

    public VRSContext getVrsContext() {
        return vrsContext;
    }

    public String[] getPreferredSEs() {
        return preferredSEsList.split(",");
    }

    public void setPreferredSEs() {
        GlobalConfig.setSystemProperty("lfc.listPreferredSEs", preferredSEsList);
    }
    
    public void setPreferredSEs(String list) {
        GlobalConfig.setSystemProperty("lfc.listPreferredSEs", list);
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
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

    public boolean useLcgCommands() {
        return useLcgCommands;
    }

    public String getVo() {
        return vo;
    }

    public List<String> getFailoverServers() {
        return failoverServers;
    }
}
