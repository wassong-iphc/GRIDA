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
package fr.insalyon.creatis.agent.vlet.execution.operation;

import fr.insalyon.creatis.agent.vlet.Configuration;
import fr.insalyon.creatis.agent.vlet.common.bean.GridData;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class LCGOperations {

    private final static Logger logger = Logger.getLogger(LCGOperations.class);

    /**
     * 
     * @param logger
     * @param localFilePath
     * @param remoteDir
     * @return
     * @throws Exception 
     */
    public static String uploadFile(String proxy,
            String localFilePath, String remoteDir) throws Exception {

        String localPath = "file:" + localFilePath;
        String fileName = new File(localFilePath).getName();
        String lfn = "lfn:" + remoteDir + "/" + fileName;
        boolean completed = false;

        logger.info("Uploading file: " + localFilePath + " - To: " + lfn);

        for (String se : Configuration.getInstance().getPreferredSEs()) {

            Process process = OperationsUtil.getProcess(proxy, "lcg-cr", "-v",
                    "--connect-timeout", "10", "--sendreceive-timeout", "900",
                    "--bdii-timeout", "10", "--srm-timeout", "30",
                    "--vo", Configuration.getInstance().getVo(),
                    "-d", se, "-l", lfn, localPath);

            BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String s = null;
            String cout = "";

            while ((s = r.readLine()) != null) {
                cout += s + "\n";
            }
            process.waitFor();
            close(process);
            r.close();

            if (process.exitValue() != 0) {
                logger.error(cout);
            } else {
                completed = true;
                break;
            }
            process = null;
        }

        if (!completed) {
            throw new Exception("Failed to perform upload from LCG command.");
        }

        return remoteDir + "/" + fileName;
    }

    /**
     * 
     * @param logger
     * @param localDirPath
     * @param fileName
     * @param remoteFilePath
     * @return
     * @throws Exception 
     */
    public static String downloadFile(String proxy, String localDirPath,
            String fileName, String remoteFilePath) throws Exception {

        String lfn = "lfn:" + remoteFilePath;
        String localPath = "file:" + localDirPath + "/" + fileName;

        logger.info("Downloading: " + lfn + " - To: " + localPath);

        Process process = OperationsUtil.getProcess(proxy, "lcg-cp", "-v",
                "--connect-timeout", "10", "--sendreceive-timeout", "900",
                "--bdii-timeout", "10", "--srm-timeout", "30",
                "--vo", Configuration.getInstance().getVo(),
                lfn, localPath);

        BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String s = null;
        String cout = "";

        while ((s = r.readLine()) != null) {
            cout += s + "\n";
        }
        process.waitFor();
        close(process);
        r.close();

        if (process.exitValue() != 0) {
            logger.error(cout);
            File file = new File(localDirPath + "/" + fileName);
            if (file.exists()) {
                file.delete();
            }
            throw new Exception(cout);
        }
        process = null;

        return localDirPath + "/" + fileName;
    }

    /**
     * 
     * @param logger
     * @param proxy
     * @param sourcePath
     * @throws Exception 
     */
    public static boolean replicateFile(String proxy,
            String sourcePath) throws Exception {

        String lfn = "lfn:" + sourcePath;
        boolean replicated = false;

        for (String se : Configuration.getInstance().getPreferredSEs()) {
            logger.info("Replicating: " + lfn + " - To: " + se);

            Process process = OperationsUtil.getProcess(proxy, "lcg-rep", "-v",
                    "--connect-timeout", "10", "--sendreceive-timeout", "900",
                    "--bdii-timeout", "10", "--srm-timeout", "30",
                    "--vo", Configuration.getInstance().getVo(),
                    "-d", se, lfn);

            BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String s = null;
            String cout = "";

            while ((s = r.readLine()) != null) {
                cout += s + "\n";
            }
            process.waitFor();
            close(process);
            r.close();

            if (process.exitValue() != 0) {
                logger.warn("Unable to replicate file to '" + se + "': " + cout);
            } else {
                replicated = true;
            }
            process = null;
        }
        return replicated;
    }

    /**
     * 
     * @param logger
     * @param proxy
     * @param path
     * @return
     * @throws Exception 
     */
    public static boolean deleteFile(String proxy, String path) throws Exception {

        try {
            LCGFailoverOperations.deleteFile(proxy, path);
        } catch (Exception ex) {
        }

        String lfn = "lfn:" + path;
        boolean deleted = false;

        logger.info("Deleting '" + lfn + "'");
        Process process = OperationsUtil.getProcess(proxy, "lcg-del", "-v",
                "-a", lfn);

        BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String s = null;
        String cout = "";

        while ((s = r.readLine()) != null) {
            cout += s + "\n";
        }
        process.waitFor();
        close(process);
        r.close();

        if (process.exitValue() != 0) {
            if (cout.contains("No such file or directory")) {
                deleted = true;
            } else {
                logger.warn("Unable to delete file '" + lfn + "': " + cout);
            }
        } else {
            deleted = true;
        }
        process = null;

        return deleted;
    }

    /**
     * 
     * @param proxy
     * @param path
     * @throws Exception 
     */
    public static boolean deleteFolder(String proxy, String path) throws Exception {

        String lfn = "lfn:" + path;

        logger.info("Deleting folder '" + lfn + "'");
        String SEP = "--";
        boolean contentsDeleted = true;

        List<String> contents = listFilesAndFolders(proxy, path);
        for (String content : contents) {
            String[] data = content.split(SEP);
            boolean deleted = false;
            if (data[1].equals(GridData.Type.Folder.name())) {
                deleted = deleteFolder(proxy, path + "/" + data[0]);
            } else {
                deleted = deleteFile(proxy, path + "/" + data[0]);
            }
            if (!deleted) {
                contentsDeleted = false;
            }
        }

        if (contentsDeleted) {
            Process process = OperationsUtil.getProcess(proxy, "lfc-rm", "-r", path);
            BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String s = null;
            String cout = "";

            while ((s = r.readLine()) != null) {
                cout += s + "\n";
            }
            process.waitFor();
            close(process);
            r.close();

            if (process.exitValue() != 0) {
                logger.error("Unable to delete folder '" + path + "': " + cout);
            }
            process = null;
        }

        return contentsDeleted;
    }

    /**
     * 
     * @param proxy
     * @param path
     * @return
     * @throws Exception 
     */
    public static List<String> listFilesAndFolders(String proxy, String path)
            throws Exception {

        logger.info("Listing contents of: " + path);
        Process process = OperationsUtil.getProcess(proxy, "lfc-ls", "-l", path);

        BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String s = null;
        String cout = "";
        List<String> data = new ArrayList<String>();
        String SEP = "--";

        while ((s = r.readLine()) != null) {
            cout += s + "\n";
            String[] line = s.split("\\s+");
            GridData.Type type = line[0].startsWith("d") ? GridData.Type.Folder : GridData.Type.File;

            StringBuilder dataName = new StringBuilder();

            for (int i = 8; i < line.length; i++) {
                if (dataName.length() > 0) {
                    dataName.append(" ");
                }
                dataName.append(line[i]);
            }

            if (type == GridData.Type.Folder) {
                data.add(dataName.toString() + SEP + type + SEP + line[0]);
            } else {
                String modifTime = line[5] + " " + line[6] + " " + line[7];
                data.add(dataName.toString() + SEP + type + SEP + line[4] + SEP
                        + modifTime + SEP + "-" + SEP + line[0]);
            }
        }
        process.waitFor();
        close(process);
        r.close();

        if (process.exitValue() != 0) {
            logger.error("Unable to list folder '" + path + "': " + cout);
            throw new Exception(cout);
        }
        process = null;

        return data;
    }

    /**
     * 
     * @param proxy
     * @param path
     * @return
     * @throws Exception 
     */
    public static long getModificationDate(String proxy, String path) throws Exception {

        Process process = OperationsUtil.getProcess(proxy, "lfc-ls", "-ld", path);
        BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String s = null;
        String cout = "";
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MMM/dd HH:mm");
        Date modifTime = null;

        while ((s = r.readLine()) != null) {
            cout += s + "\n";
        }
        process.waitFor();
        close(process);
        r.close();

        if (process.exitValue() != 0) {
            logger.error("Unable to get modification date for '" + path + "': " + cout);
            throw new Exception(cout);
        }

        process = null;

        String[] line = cout.split("\\s+");

        if (line[7].contains(":")) {
            modifTime = formatter.parse(
                    Calendar.getInstance().get(Calendar.YEAR) + "/"
                    + line[5] + "/" + line[6] + " " + line[7]);

        } else {
            modifTime = formatter.parse(
                    line[7] + "/" + line[5] + "/" + line[6] + " 00:00");
        }
        return modifTime.getTime();
    }

    /**
     * 
     * @param proxy
     * @param path
     * @throws Exception 
     */
    public static void createFolder(String proxy, String path) throws Exception {

        logger.info("Creating folder: " + path);

        if (!exists(proxy, path)) {

            Process process = OperationsUtil.getProcess(proxy, "lfc-mkdir", "-p", path);
            BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String s = null;
            String cout = "";

            while ((s = r.readLine()) != null) {
                cout += s + "\n";
            }
            process.waitFor();
            close(process);
            r.close();

            if (process.exitValue() != 0) {
                logger.error("Unable to create folder '" + path + "': " + cout);
                throw new Exception("Unable to create folder '" + path + "': " + cout);
            }
            process = null;
        }
    }

    /**
     * 
     * @param proxy
     * @param path
     * @return
     * @throws Exception 
     */
    public static boolean exists(String proxy, String path) throws Exception {

        Process process = OperationsUtil.getProcess(proxy, "lfc-ls", "-ld", path);
        BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String s = null;
        String cout = "";

        while ((s = r.readLine()) != null) {
            cout += s + "\n";
        }
        process.waitFor();
        close(process);
        r.close();

        if (process.exitValue() != 0) {

            if (cout.contains("No such file or directory")) {
                return false;
            }
            logger.error("Unable verify data existence for '" + path + "': " + cout);
            throw new Exception("Unable verify data existence for '" + path + "': " + cout);
        }
        process = null;

        return true;
    }

    /**
     * 
     * @param proxy
     * @param path
     * @return
     * @throws Exception 
     */
    public static boolean isDir(String proxy, String path) throws Exception {

        Process process = OperationsUtil.getProcess(proxy, "lfc-ls", "-ld", path);
        BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String s = null;
        String cout = "";

        while ((s = r.readLine()) != null) {
            cout += s + "\n";
        }
        process.waitFor();
        close(process);
        r.close();

        if (process.exitValue() != 0) {
            logger.error("Unable verify data for '" + path + "': " + cout);
            throw new Exception("Unable verify data for '" + path + "': " + cout);

        }
        process = null;

        String[] line = cout.split("\\s+");
        return line[0].startsWith("d") ? true : false;
    }

    private static void close(Process process) throws Exception {

        close(process.getOutputStream());
        close(process.getInputStream());
        close(process.getErrorStream());
        process.destroy();
    }

    private static void close(Closeable c) throws Exception {
        if (c != null) {
            try {
                c.close();
            } catch (IOException ex) {
                // ignored
            }
        }
    }
}
