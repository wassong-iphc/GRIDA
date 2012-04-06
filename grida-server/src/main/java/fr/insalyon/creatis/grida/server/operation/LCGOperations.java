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

import fr.insalyon.creatis.grida.common.bean.GridData;
import fr.insalyon.creatis.grida.server.Configuration;
import fr.insalyon.creatis.grida.server.dao.DAOException;
import fr.insalyon.creatis.grida.server.dao.DAOFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class LCGOperations {

    private final static Logger logger = Logger.getLogger(LCGOperations.class);

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    public static long getModificationDate(String proxy, String path) throws OperationException {

        try {
            logger.info("[LCG] Getting modification date for: " + path);
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
            OperationsUtil.close(process);
            r.close();

            if (process.exitValue() != 0) {
                logger.error("[LCG] Unable to get modification date for '" + path + "': " + cout);
                throw new OperationException(cout);
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

        } catch (ParseException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    public static List<GridData> listFilesAndFolders(String proxy, String path)
            throws OperationException {

        try {
            logger.info("[LCG] Listing contents of: " + path);
            Process process = OperationsUtil.getProcess(proxy, "lfc-ls", "-l", path);

            BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String s = null;
            String cout = "";
            List<GridData> data = new ArrayList<GridData>();

            while ((s = r.readLine()) != null) {
                cout += s + "\n";
                String[] line = s.split("\\s+");
                GridData.Type type = line[0].startsWith("d") ? GridData.Type.Folder : GridData.Type.File;

                StringBuilder dataName = new StringBuilder();
                for (int i = 8; i < line.length; i++) {
                    if (line[i].equals("->")) {
                        break;
                    }
                    if (dataName.length() > 0) {
                        dataName.append(" ");
                    }
                    dataName.append(line[i]);
                }

                if (type == GridData.Type.Folder) {
                    data.add(new GridData(dataName.toString(), type, line[0]));
                } else {
                    String modifTime = "unknown";
                    if (line.length < 6) {
                        logger.warn("Cannot get modification time; setting it to \"unknown\".");
                    } else {
                        modifTime = line[5] + " " + line[6] + " " + line[7];
                    }
                    Long l = null;
                    try {
                        l = new Long(line[4]);
                    } catch (java.lang.NumberFormatException e) {
                        logger.warn("Cannot parse long: \"" + line[4] + "\". Setting file length to 0");
                        l = new Long(0);
                    } catch (java.lang.ArrayIndexOutOfBoundsException e) {
                        logger.warn("Cannot get long. Setting file length to 0");
                        l = new Long(0);
                    }

                    data.add(new GridData(dataName.toString(), type,
                            l, modifTime, "-", line[0]));
                }
            }
            process.waitFor();
            OperationsUtil.close(process);
            r.close();

            if (process.exitValue() != 0) {
                logger.error("[LCG] Unable to list folder '" + path + "': " + cout);
                throw new OperationException(cout);
            }
            process = null;

            return data;

        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param localDirPath
     * @param fileName
     * @param remoteFilePath
     * @return
     * @throws OperationException
     */
    public static String downloadFile(String proxy, String localDirPath,
            String fileName, String remoteFilePath) throws OperationException {

        try {
            String lfn = "lfn:" + remoteFilePath;
            String localPath = "file:" + localDirPath + "/" + fileName;

            logger.info("[LCG] Downloading: " + lfn + " - To: " + localPath);

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
            OperationsUtil.close(process);
            r.close();

            if (process.exitValue() != 0) {
                logger.error(cout);
                File file = new File(localDirPath + "/" + fileName);
                FileUtils.deleteQuietly(file);
                throw new OperationException(cout);
            }
            process = null;

            return localDirPath + "/" + fileName;

        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param localFilePath
     * @param remoteDir
     * @return
     * @throws OperationException
     */
    public static String uploadFile(String proxy, String localFilePath,
            String remoteDir) throws OperationException {

        try {
            String localPath = "file:" + localFilePath;
            String fileName = new File(localFilePath).getName();
            String lfn = "lfn:" + remoteDir + "/" + fileName;
            boolean completed = false;

            logger.info("[LCG] Uploading file: " + localFilePath + " - To: " + lfn);

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
                OperationsUtil.close(process);
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
                throw new OperationException("Failed to perform upload from LCG command.");
            }

            FileUtils.deleteQuietly(new File(localFilePath));
            return remoteDir + "/" + fileName;

        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param sourcePath
     * @return
     * @throws OperationException
     */
    public static void replicateFile(String proxy, String sourcePath)
            throws OperationException {

        try {
            String lfn = "lfn:" + sourcePath;

            for (String se : Configuration.getInstance().getPreferredSEs()) {
                logger.info("[LCG] Replicating: " + lfn + " - To: " + se);

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
                OperationsUtil.close(process);
                r.close();

                if (process.exitValue() != 0) {
                    logger.error("Unable to replicate file to '" + se + "': " + cout);
                    throw new OperationException(cout);
                }
                process = null;
            }

        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    public static boolean isDir(String proxy, String path) throws OperationException {

        try {
            Process process = OperationsUtil.getProcess(proxy, "lfc-ls", "-ld", path);
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
                logger.error("Unable verify data for '" + path + "': " + cout);
                throw new OperationException("Unable verify data for '" + path + "': " + cout);

            }
            process = null;

            String[] line = cout.split("\\s+");
            return line[0].startsWith("d") ? true : false;

        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @throws Operation Exception
     */
    public static void deleteFolder(String proxy, String path) throws OperationException {

        try {
            String lfn = "lfn:" + path;
            logger.info("[LCG] Deleting folder '" + lfn + "'.");

            for (GridData data : listFilesAndFolders(proxy, path)) {
                try {
                    if (data.getType() == GridData.Type.Folder) {
                        deleteFolder(proxy, path + "/" + data.getName());
                    } else {
                        deleteFile(proxy, path + "/" + data.getName());
                    }
                } catch (OperationException ex) {
                }
            }

            Process process = OperationsUtil.getProcess(proxy, "lfc-rm", "-r", path);
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
                logger.error("Unable to delete folder '" + path + "': " + cout);
                throw new OperationException(cout);
            }
            process = null;

        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    public static void deleteFile(String proxy, String path) throws OperationException {

        try {
            try {
                LCGFailoverOperations.deleteFile(proxy, path);
            } catch (Exception ex) {
            }

            String lfn = "lfn:" + path;

            logger.info("Deleting '" + lfn + "'");
            Process process = OperationsUtil.getProcess(proxy, "lcg-del", "-v", 
                    "-a", "--sendreceive-timeout", "30", lfn);

            BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String s = null;
            String cout = "";

            while ((s = r.readLine()) != null) {
                cout += s + "\n";
            }
            process.waitFor();
            OperationsUtil.close(process);
            r.close();

            if (process.exitValue() != 0 && !cout.contains("No such file or directory")) {

                if (cout.contains("SRM_INVALID_PATH")) {
                    unregister(proxy, path, getGUID(proxy, path), getSURL(proxy, path));

                } else {
                    logger.error("Unable to delete file '" + lfn + "': " + cout);
                    throw new OperationException(cout);
                }
            }
            process = null;

        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @throws Exception
     */
    public static void createFolder(String proxy, String path) throws OperationException {

        try {
            logger.info("[LCG] Creating folder: " + path);

            if (!exists(proxy, path)) {

                Process process = OperationsUtil.getProcess(proxy, "lfc-mkdir", "-p", path);
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
                    logger.error("Unable to create folder '" + path + "': " + cout);
                    throw new OperationException(cout);
                }
                process = null;
            }
        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param oldPath
     * @param newPath
     * @throws OperationException
     */
    public static void rename(String proxy, String oldPath, String newPath) throws OperationException {

        try {
            logger.info("[LCG] Renaming '" + oldPath + "' to '" + newPath + "'.");

            if (exists(proxy, newPath)) {
                logger.warn("[LCG] File " + newPath + " already exists. Trying with new name.");
                newPath += new SimpleDateFormat("-yyyy-MM-dd_HH-mm-ss").format(new Date());
            }
            Process process = OperationsUtil.getProcess(proxy, "lfc-rename", oldPath, newPath);
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
                logger.error("Unable to rename '" + oldPath + "': " + cout);
                throw new OperationException(cout);
            }
            process = null;

        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws Exception
     */
    public static boolean exists(String proxy, String path) throws OperationException {

        try {
            Process process = OperationsUtil.getProcess(proxy, "lfc-ls", "-ld", path);
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
                if (cout.contains("No such file or directory")) {
                    return false;
                }
                logger.error("Unable verify data existence for '" + path + "': " + cout);
                throw new OperationException(cout);
            }
            process = null;

            return true;

        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    public static long getFileSize(String proxy, String path) throws OperationException {

        try {
            logger.info("[LCG] getting size of: " + path);
            Process process = OperationsUtil.getProcess(proxy, "lfc-ls", "-l", path);

            BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String s = null;
            String cout = "";

            long size = 0;
            while ((s = r.readLine()) != null) {
                cout += s + "\n";
                String[] line = s.split("\\s+");
                size = new Long(line[4]);
            }
            process.waitFor();
            OperationsUtil.close(process);
            r.close();

            if (process.exitValue() != 0) {
                logger.error("[LCG] Unable to get size of '" + path + "': " + cout);
                throw new OperationException(cout);
            }
            process = null;

            return size;

        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    private static String getGUID(String proxy, String path) throws OperationException {

        try {
            logger.info("[LCG] Getting GUID of: " + path);
            Process process = OperationsUtil.getProcess(proxy, "lcg-lg", "lfn:" + path);

            BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String s = null;
            String cout = "";

            String guid = null;
            while ((s = r.readLine()) != null) {
                cout += s + "\n";
                guid = s;
            }
            process.waitFor();
            OperationsUtil.close(process);
            r.close();

            if (process.exitValue() != 0) {
                logger.error("[LCG] Unable to get GUID of '" + path + "': " + cout);
                throw new OperationException(cout);
            }
            process = null;

            return guid;

        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @return
     * @throws OperationException
     */
    private static String[] getSURL(String proxy, String path) throws OperationException {

        try {
            logger.info("[LCG] Getting SURL of: " + path);
            Process process = OperationsUtil.getProcess(proxy, "lcg-lr", "lfn:" + path);

            BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String s = null;
            String cout = "";

            List<String> surls = new ArrayList<String>();
            while ((s = r.readLine()) != null) {
                cout += s + "\n";
                surls.add(s);
            }
            process.waitFor();
            OperationsUtil.close(process);
            r.close();

            if (process.exitValue() != 0) {
                logger.error("[LCG] Unable to get SURL of '" + path + "': " + cout);
                throw new OperationException(cout);
            }
            process = null;

            return surls.toArray(new String[]{});

        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }

    /**
     *
     * @param proxy
     * @param path
     * @param guid
     * @param surls
     * @throws OperationException
     */
    private static void unregister(String proxy, String path, String guid,
            String... surls) throws OperationException {

        try {
            logger.info("[LCG] Unregistering: " + path);
            for (String surl : surls) {

                Process process = OperationsUtil.getProcess(proxy, "lcg-uf", "-v", guid, surl);

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
                    logger.error("[LCG] Unable to unregister '" + path + "': " + cout);
                    throw new OperationException(cout);
                }
                process = null;
                DAOFactory.getDAOFactory().getZombieFilesDAO().add(surl);
            }

        } catch (DAOException ex) {
            throw new OperationException(ex);
        } catch (InterruptedException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        } catch (IOException ex) {
            logger.error(ex);
            throw new OperationException(ex);
        }
    }
}
