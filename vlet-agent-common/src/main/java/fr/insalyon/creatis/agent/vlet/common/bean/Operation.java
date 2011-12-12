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
package fr.insalyon.creatis.agent.vlet.common.bean;

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
    public Operation(String id, Date registration, String source, String dest, String type, String status, String user) {
        this(id, registration, source, dest, type, status, user, "", 0);
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
    public Operation(String id, String source, String dest, String type, String user, String proxy) {
        this(id, new Date(), source, dest, type, Status.Queued.toString(), user, proxy, 0);
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
     */
    public Operation(String id, Date registration, String source, String dest, String type, String status, String user, String proxy, int retrycount) {
        this.id = id;
        this.registration = registration;
        this.source = source;
        this.dest = dest;
        this.type = Type.valueOf(type);
        this.status = Status.valueOf(status);
        this.user = user;
        this.proxy = proxy;
        this.retrycount = retrycount;
    }

    public String getId() {
        return id;
    }

    public Date getRegistration() {
        return registration;
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
}
