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
package fr.insalyon.creatis.grida.common.bean;

import fr.insalyon.creatis.grida.common.Constants;
import java.io.File;
import java.util.Date;

/**
 *
 * @author Rafael Silva
 */
public class Operation {

    public static enum Type {

        Upload, Download, Download_Files, Delete, Replicate
    };

    public static enum Status {

        Queued, Running, Done, Failed, Rescheduled
    };
    private String id;
    private Date registration;
    private String source;
    private String dest;
    private Type type;
    private Status status;
    private String user;
    private String proxy;
    private int retrycount;
    private double size;
    private int progress;

    /**
     *
     * @param id
     * @param registration
     * @param source
     * @param dest
     * @param type
     * @param status
     * @param user
     */
    public Operation(String id, Date registration, String source, String dest,
            String type, String status, String user, double size, int progress) {
        this(id, registration, source, dest, type, status, user, "", 0, size, progress);
    }

    /**
     *
     * @param id
     * @param source
     * @param dest
     * @param type
     * @param user
     * @param proxy
     */
    public Operation(String id, String source, String dest, String type,
            String user, String proxy, double size) {
        this(id, new Date(), source, dest, type, Status.Queued.toString(), user, proxy, 0, size);
    }

    /**
     *
     * @param id
     * @param registration
     * @param source
     * @param dest
     * @param type
     * @param status
     * @param user
     * @param proxy
     * @param retrycount
     * @param size
     */
    public Operation(String id, Date registration, String source, String dest,
            String type, String status, String user, String proxy,
            int retrycount, double size) {
        this(id, registration, source, dest, type, status, user, proxy, retrycount, size, 0);
    }

    /**
     *
     * @param id
     * @param registration
     * @param source
     * @param dest
     * @param type
     * @param status
     * @param user
     * @param proxy
     * @param retrycount
     * @param size
     * @param progress
     */
    public Operation(String id, Date registration, String source, String dest,
            String type, String status, String user, String proxy,
            int retrycount, double size, int progress) {

        this.id = id;
        this.registration = registration;
        this.source = source;
        this.dest = dest;
        this.type = Type.valueOf(type);
        this.status = Status.valueOf(status);
        this.user = user;
        this.proxy = proxy;
        this.retrycount = retrycount;
        this.size = size;
        this.progress = progress;
    }

    public String getId() {
        return id;
    }

    public Date getRegistration() {
        return registration;
    }

    public void setRegistration(Date registration) {
        this.registration = registration;
    }

    public String getSource() {
        return source;
    }

    public String getDest() {
        return dest;
    }

    public Type getType() {
        return type;
    }

    public String getProxy() {
        return proxy;
    }

    public Status getStatus() {
        return status;
    }

    public String getUser() {
        return user;
    }

    public int getRetrycount() {
        return retrycount;
    }

    public void incrementRetryCount() {
        this.retrycount++;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public double getSize() {
        return size;
    }

    public int getProgress() {
        return progress;
    }

    @Override
    public String toString() {

        if (type == Type.Download) {
            File destFile = new File(dest + "/" + new File(source).getName());
            if (destFile.exists()) {
                progress = (int) (destFile.length() * 100 / size);
            }

        } else if (type == Type.Download_Files) {

            long downloaded = 0;
            for (String src : source.split(Constants.MSG_SEP_2)) {
                File destFile = new File(dest + "/" + new File(src).getName());
                if (destFile.exists()) {
                    downloaded += destFile.length();
                }
            }
            progress = (int) (downloaded * 100 / size);
        }

        return id
                + Constants.MSG_SEP_2 + registration.getTime()
                + Constants.MSG_SEP_2 + source.replaceAll(Constants.MSG_SEP_2, Constants.MSG_SEP_3)
                + Constants.MSG_SEP_2 + dest
                + Constants.MSG_SEP_2 + type.name()
                + Constants.MSG_SEP_2 + status.name()
                + Constants.MSG_SEP_2 + user
                + Constants.MSG_SEP_2 + size
                + Constants.MSG_SEP_2 + progress;
    }
}
