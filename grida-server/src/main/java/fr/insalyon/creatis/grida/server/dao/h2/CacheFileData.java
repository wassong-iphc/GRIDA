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
package fr.insalyon.creatis.grida.server.dao.h2;

import fr.insalyon.creatis.grida.server.dao.CacheFileDAO;
import fr.insalyon.creatis.grida.server.dao.DAOException;
import fr.insalyon.creatis.grida.common.bean.CachedFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class CacheFileData implements CacheFileDAO {

    private final static Logger logger = Logger.getLogger(CacheFileData.class);
    private static CacheFileData instance;
    private Connection connection;

    public synchronized static CacheFileData getInstance(Connection connection) {
        if (instance == null) {
            instance = new CacheFileData(connection);
        }
        return instance;
    }

    private CacheFileData(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void add(CachedFile cacheFile) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO CacheFile(path, name, size, "
                    + "frequency, last_usage) "
                    + "VALUES (?, ?, ?, ?, ?)");

            ps.setString(1, cacheFile.getPath());
            ps.setString(2, cacheFile.getName());
            ps.setDouble(3, cacheFile.getSize());
            ps.setInt(4, cacheFile.getFrequency());
            ps.setTimestamp(5, new Timestamp(cacheFile.getLastUsage().getTime()));
            ps.execute();

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public void update(String path) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("UPDATE CacheFile "
                    + "SET frequency = frequency + 1, last_usage = ? "
                    + "WHERE path = ?");
            ps.setTimestamp(1, new Timestamp(new Date().getTime()));
            ps.setString(2, path);
            ps.executeUpdate();

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public void delete(String path) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("DELETE "
                    + "FROM CacheFile WHERE path = ?");

            ps.setString(1, path);
            ps.execute();

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public List<String> delete(double size) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("SELECT "
                    + "path, name, size FROM CacheFile "
                    + "ORDER BY last_usage, frequency");
            ResultSet rs = ps.executeQuery();

            boolean stop = false;
            double missingSize = size;
            List<String> deletedEntries = new ArrayList<String>();

            while (rs.next() && !stop) {
                String p = rs.getString("path");
                String n = rs.getString("name");
                double s = rs.getDouble("size");

                ps = connection.prepareStatement("DELETE "
                        + "FROM CacheFile WHERE path = ?");
                ps.setString(1, p);
                ps.execute();
                deletedEntries.add(n);

                missingSize -= s;
                if (missingSize <= 0) {
                    stop = true;
                }
            }
            return deletedEntries;

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public String getName(String path) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("SELECT "
                    + "name FROM CacheFile "
                    + "WHERE path = ?");

            ps.setString(1, path);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString("name");
            } else {
                return null;
            }
        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public double getTotalUsedSpace() throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("SELECT "
                    + "SUM(size) AS totalspace FROM CacheFile");
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getDouble("totalspace");
            } else {
                return 0;
            }
        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public List<CachedFile> getFiles() throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement(
                    "SELECT path, name, size, frequency, last_usage "
                    + "FROM CacheFile");

            ResultSet rs = ps.executeQuery();
            List<CachedFile> cacheFiles = new ArrayList<CachedFile>();

            while (rs.next()) {
                cacheFiles.add(new CachedFile(
                        rs.getString("path"),
                        rs.getString("name"),
                        rs.getDouble("size"),
                        rs.getInt("frequency"),
                        new Date(rs.getTimestamp("last_usage").getTime())));
            }

            return cacheFiles;

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public CachedFile getFile(String path) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement(
                    "SELECT path, name, size, frequency, last_usage "
                    + "FROM CacheFile WHERE path=?");

            ps.setString(1, path);
            ResultSet rs = ps.executeQuery();
            rs.next();

            return new CachedFile(
                    rs.getString("path"),
                    rs.getString("name"),
                    rs.getDouble("size"),
                    rs.getInt("frequency"),
                    new Date(rs.getTimestamp("last_usage").getTime()));

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }
}
