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

/**
 *
 * @author Rafael Ferreira da Silva
 */
public class Constants {

    // Configuration Labels
    public static final String LAB_AGENT_PORT = "agent.port";
    public static final String LAB_AGENT_RETRYCOUNT = "agent.retrycount";
    public static final String LAB_AGENT_MIN_AVAILABLE_DISKSPACE = "agent.min.available.diskspace";
    public static final String LAB_BDII_HOST = "bdii.host";
    public static final String LAB_BDII_PORT = "bdii.port";
    public static final String LAB_CACHE_MAX_ENTRIES = "cache.list.max.entries";
    public static final String LAB_CACHE_MAX_HOURS = "cache.list.max.hours";
    public static final String LAB_CACHE_MAX_SIZE = "cache.files.max.size";
    public static final String LAB_CACHE_PATH = "cache.files.path";
    public static final String LAB_FAILOVER_SERVERS = "failover.servers";
    public static final String LAB_LFC_HOST = "lfc.host";
    public static final String LAB_LFC_PREFERRED_SES = "lfc.preferredSEsList";
    public static final String LAB_POOL_MAX_DELETE = "pool.max.delete";
    public static final String LAB_POOL_MAX_DOWNLOAD = "pool.max.download";
    public static final String LAB_POOL_MAX_HISTORY = "pool.max.history";
    public static final String LAB_POOL_MAX_REPLICATION = "pool.max.replication";
    public static final String LAB_POOL_MAX_UPLOAD = "pool.max.upload";
    public static final String LAB_VO = "vo";
}
