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
package fr.insalyon.creatis.grida.server.operation;

import fr.insalyon.creatis.grida.server.Configuration;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class LCGFailoverOperations {

    private static Logger logger = Logger.getLogger(LCGFailoverOperations.class);

    /**
     *
     * @param proxy
     * @param localDirPath
     * @param fileName
     * @param remoteFilePath
     * @return
     * @throws Exception
     */
    public static String downloadFile(String proxy, String localDirPath,
            String fileName, String remoteFilePath) throws Exception {

        if (hasFailoverServer()) {
            String lfn = "lfn:" + remoteFilePath;
            String localPath = "file:" + localDirPath + "/" + fileName;

            List<String> paths = getReplicas(proxy, lfn);

            logger.info("Downloading File from Failover Servers: " + lfn + " - To: " + localPath);
            boolean downloaded = false;

            for (String path : paths) {
                Process process = OperationsUtil.getProcess(proxy, "lcg-cp", "-v", "--nobdii",
                        "--connect-timeout", "10", "--sendreceive-timeout", "900",
                        "--bdii-timeout", "10", "--srm-timeout", "30",
                        "--vo", Configuration.getInstance().getVo(),
                        "--defaultsetype", "srmv2", "-v", path,
                        localPath);

                BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String s = null;
                String cout = "";

                while ((s = r.readLine()) != null) {
                    cout += s + "\n";
                }
                process.waitFor();
                OperationsUtil.close(process);
                r.close();

                if (process.exitValue() != 0) {
                    logger.error(cout);
                } else {
                    downloaded = true;
                    break;
                }
                process = null;
            }

            if (!downloaded) {
                File file = new File(localPath);
                if (file.exists()) {
                    file.delete();
                }
                logger.error("Unable to download file from failover servers.");
                throw new Exception("Unable to download file from failover servers.");
            }
        }
        return localDirPath + "/" + fileName;
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws Exception
     */
    public static void deleteFile(String proxy, String path) throws Exception {

        if (hasFailoverServer()) {

            String lfn = "lfn:" + path;

            List<String> paths = getReplicas(proxy, lfn);

            if (!paths.isEmpty()) {
                logger.info("Deleting File from Failover Servers: " + lfn);

                for (String p : paths) {
                    Process process = OperationsUtil.getProcess(proxy, "lcg-del", "-v",
                            "--nobdii", "--defaultsetype", "srmv2", p);

                    BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    String s = null;
                    String cout = "";

                    while ((s = r.readLine()) != null) {
                        cout += s + "\n";
                    }
                    process.waitFor();
                    OperationsUtil.close(process);
                    r.close();

                    if (process.exitValue() != 0) {
                        logger.warn("Unable to delete file '" + lfn + "': " + cout);
                    }
                    process = null;
                }
            }
        }
    }

    /**
     *
     * @return @throws Exception
     */
    private static boolean hasFailoverServer() throws Exception {
        return !Configuration.getInstance().getFailoverServers().isEmpty();
    }

    /**
     *
     * @param proxy
     * @param lfn
     * @return
     * @throws Exception
     */
    private static List<String> getReplicas(String proxy, String lfn) throws Exception {

        Process process = OperationsUtil.getProcess(proxy, "lcg-lr", lfn);

        BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String s = null;
        String cout = "";
        List<String> paths = new ArrayList<String>();

        while ((s = r.readLine()) != null) {
            cout += s + "\n";
            for (String failoverServer : Configuration.getInstance().getFailoverServers()) {
                if (s.contains(new URI(failoverServer).getHost())) {
                    paths.add(failoverServer + "?SFN=" + new URI(s).getPath());
                }
            }
        }
        process.waitFor();
        OperationsUtil.close(process);
        r.close();

        if (process.exitValue() != 0) {
            logger.error(cout);
            throw new Exception(cout);
        }
        process = null;

        return paths;
    }
}
