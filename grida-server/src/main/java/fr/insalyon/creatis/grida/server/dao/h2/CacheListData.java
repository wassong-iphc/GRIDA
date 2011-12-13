/* Copyright CNRS-CREATIS
 *
 * Rafael Silva
 * rafael.silva@creatis.insa-lyon.fr
 * http://www.creatis.insa-lyon.fr/~silva
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
package fr.insalyon.creatis.grida.server.dao.h2;

import fr.insalyon.creatis.grida.server.dao.CacheListDAO;
import fr.insalyon.creatis.grida.server.dao.DAOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Rafael Silva
 */
public class CacheListData implements CacheListDAO {

    private static CacheListData instance;
    private final String SEPARATOR = "###";
    private Connection connection;

    public synchronized static CacheListData getInstance(Connection connection) {
        if (instance == null) {
            instance = new CacheListData(connection);
        }
        return instance;
    }

    private CacheListData(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void add(String path, List<String> pathList) throws DAOException {
        try {
            StringBuilder sb = new StringBuilder();
            for (String s : pathList) {
                if (sb.length() > 0) {
                    sb.append(SEPARATOR);
                }
                sb.append(s);
            }
            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO CacheList(path, list, frequency, "
                    + "last_usage, last_refresh) "
                    + "VALUES (?, ?, ?, ?, ?)");

            ps.setString(1, path);
            ps.setString(2, sb.toString());
            ps.setInt(3, 1);
            ps.setTimestamp(4, new Timestamp(new Date().getTime()));
            ps.setTimestamp(5, new Timestamp(new Date().getTime()));
            ps.execute();

        } catch (SQLException ex) {
            throw new DAOException(ex);
        }
    }

    @Override
    public void delete() throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("SELECT "
                    + "path FROM CacheList "
                    + "ORDER BY last_usage, frequency "
                    + "LIMIT 0, 1");
            ResultSet rs = ps.executeQuery();
            rs.next();
            String path = rs.getString("path");

            ps = connection.prepareStatement("DELETE "
                    + "FROM CacheList WHERE path = ?");

            ps.setString(1, path);
            ps.execute();

        } catch (SQLException ex) {
            throw new DAOException(ex);
        }
    }

    @Override
    public void update(String path) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("SELECT "
                    + "frequency FROM CacheList "
                    + "WHERE path = ?");

            ps.setString(1, path);
            ResultSet rs = ps.executeQuery();
            rs.next();
            int frequency = rs.getInt("frequency");

            ps = connection.prepareStatement("UPDATE CacheList SET "
                    + "frequency = ?, last_usage = ? "
                    + "WHERE path = ?");
            ps.setInt(1, frequency + 1);
            ps.setTimestamp(2, new Timestamp(new Date().getTime()));
            ps.setString(3, path);
            ps.executeUpdate();

        } catch (SQLException ex) {
            throw new DAOException(ex);
        }
    }

    @Override
    public void update(String path, List<String> pathList) throws DAOException {
        try {
            this.update(path);
            StringBuilder sb = new StringBuilder();
            for (String s : pathList) {
                if (sb.length() > 0) {
                    sb.append(SEPARATOR);
                }
                sb.append(s);
            }
            PreparedStatement ps = connection.prepareStatement(
                    "UPDATE CacheList SET list = ?, last_refresh = ? "
                    + "WHERE path = ?");

            ps.setString(1, sb.toString());
            ps.setTimestamp(2, new Timestamp(new Date().getTime()));
            ps.setString(3, path);
            ps.execute();
        } catch (SQLException ex) {
            throw new DAOException(ex);
        }
    }

    @Override
    public List<String> getPathList(String path) throws DAOException {
        try {
            List<String> paths = new ArrayList<String>();
            PreparedStatement ps = connection.prepareStatement("SELECT "
                    + "list FROM CacheList "
                    + "WHERE path = ?");

            ps.setString(1, path);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String list = rs.getString("list");

                if (list != null && !list.isEmpty()) {
                    paths.addAll(Arrays.asList(list.split(SEPARATOR)));
                }
                this.update(path);
                return paths;

            } else {
                return null;
            }
        } catch (SQLException ex) {
            throw new DAOException(ex);
        }
    }

    @Override
    public int getNumberOfEntries() throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("SELECT "
                    + "count(path) AS entries FROM CacheList");

            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getInt("entries");

        } catch (SQLException ex) {
            throw new DAOException(ex);
        }
    }

    @Override
    public Date getLastRefresh(String path) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("SELECT "
                    + "last_refresh FROM CacheList "
                    + "WHERE path = ?");
            ps.setString(1, path);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new Date(rs.getTimestamp("last_refresh").getTime());
            } else {
                return null;
            }
        } catch (SQLException ex) {
            throw new DAOException(ex);
        }
    }
}
