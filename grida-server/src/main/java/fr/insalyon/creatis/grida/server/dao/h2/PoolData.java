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

import fr.insalyon.creatis.grida.common.bean.Operation;
import fr.insalyon.creatis.grida.common.bean.Operation.Status;
import fr.insalyon.creatis.grida.common.bean.Operation.Type;
import fr.insalyon.creatis.grida.server.dao.DAOException;
import fr.insalyon.creatis.grida.server.dao.PoolDAO;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author Rafael Silva
 */
public class PoolData implements PoolDAO {

    private final static Logger logger = Logger.getLogger(PoolData.class);
    private static PoolData instance;
    private Connection connection;

    public synchronized static PoolData getInstance(Connection connection) {
        if (instance == null) {
            instance = new PoolData(connection);
        }
        return instance;
    }

    private PoolData(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void addOperation(Operation operation) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO Pool(id, registration, source, dest, "
                    + "operation, status, username, proxy, retrycount, size) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            ps.setString(1, operation.getId());
            ps.setTimestamp(2, new Timestamp(operation.getRegistration().getTime()));
            ps.setString(3, operation.getSource());
            ps.setString(4, operation.getDest());
            ps.setString(5, operation.getType().name());
            ps.setString(6, operation.getStatus().name());
            ps.setString(7, operation.getUser());
            ps.setString(8, operation.getProxy());
            ps.setInt(9, operation.getRetrycount());
            ps.setDouble(10, operation.getSize());
            ps.execute();
            ps.close();

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public void updateOperation(Operation operation) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("UPDATE Pool "
                    + "SET status=?, retrycount=?, dest=?, registration = ? "
                    + "WHERE id=?");

            ps.setString(1, operation.getStatus().name());
            ps.setInt(2, operation.getRetrycount());
            ps.setString(3, operation.getDest());
            ps.setTimestamp(4, new Timestamp(operation.getRegistration().getTime()));
            ps.setString(5, operation.getId());
            ps.executeUpdate();
            ps.close();

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public void removeOperationById(String id) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("DELETE "
                    + "FROM Pool WHERE id=?");

            ps.setString(1, id);
            ps.execute();
            ps.close();

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public List<Operation> getOperationsByUser(String user) throws DAOException {
        try {
            List<Operation> operations = new ArrayList<Operation>();
            PreparedStatement ps = connection.prepareStatement(
                    getOperationSelect()
                    + "WHERE username=? AND operation <> ? ORDER BY registration DESC");

            ps.setString(1, user);
            ps.setString(2, Operation.Type.Replicate.name()); //TODO temporary hack for the VIP portal

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                operations.add(getOperation(rs));
            }
            ps.close();
            return operations;

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public List<Operation> getDownloadPendingOperations() throws DAOException {
        try {
            List<Operation> operations = new ArrayList<Operation>();
            PreparedStatement ps = connection.prepareStatement(
                    getOperationSelect()
                    + "WHERE (status=? OR status=?) AND "
                    + "(operation=? OR operation=?) "
                    + "ORDER BY registration");

            ps.setString(1, Operation.Status.Queued.name());
            ps.setString(2, Operation.Status.Rescheduled.name());
            ps.setString(3, Operation.Type.Download.name());
            ps.setString(4, Operation.Type.Download_Files.name());

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                operations.add(getOperation(rs));
            }
            ps.close();
            return operations;

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public List<Operation> getUploadPendingOperations() throws DAOException {
        try {
            List<Operation> operations = new ArrayList<Operation>();
            PreparedStatement ps = connection.prepareStatement(
                    getOperationSelect()
                    + "WHERE (status = ? OR status = ?) AND "
                    + "operation = ? "
                    + "ORDER BY registration");

            ps.setString(1, Operation.Status.Queued.name());
            ps.setString(2, Operation.Status.Rescheduled.name());
            ps.setString(3, Operation.Type.Upload.name());

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                operations.add(getOperation(rs));
            }
            ps.close();
            return operations;

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public List<Operation> getDeletePendingOperations() throws DAOException {
        try {
            List<Operation> operations = new ArrayList<Operation>();
            PreparedStatement ps = connection.prepareStatement(
                    getOperationSelect()
                    + "WHERE (status = ? OR status = ?) AND "
                    + "operation = ? "
                    + "ORDER BY registration");

            ps.setString(1, Operation.Status.Queued.name());
            ps.setString(2, Operation.Status.Rescheduled.name());
            ps.setString(3, Operation.Type.Delete.name());

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                operations.add(getOperation(rs));
            }
            ps.close();
            return operations;

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public List<Operation> getReplicationPendingOperations() throws DAOException {
        try {
            List<Operation> operations = new ArrayList<Operation>();
            PreparedStatement ps = connection.prepareStatement(
                    getOperationSelect()
                    + "WHERE (status = ? OR status = ?) AND "
                    + "operation = ? "
                    + "ORDER BY registration");

            ps.setString(1, Operation.Status.Queued.name());
            ps.setString(2, Operation.Status.Rescheduled.name());
            ps.setString(3, Operation.Type.Replicate.name());

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                operations.add(getOperation(rs));
            }
            ps.close();
            return operations;

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public Operation getOperationById(String id) throws DAOException {
        try {

            PreparedStatement ps = connection.prepareStatement(
                    getOperationSelect() + " WHERE id=?");

            ps.setString(1, id);

            ResultSet rs = ps.executeQuery();
            rs.next();
            Operation operation = getOperation(rs);
            ps.close();
            return operation;

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public List<Operation> getAllOperations() throws DAOException {
        try {
            List<Operation> operations = new ArrayList<Operation>();
            PreparedStatement ps = connection.prepareStatement(
                    getOperationSelect() + " ORDER BY registration DESC");

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                operations.add(getOperation(rs));
            }
            ps.close();
            return operations;

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public void removeDeleteOperations() throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("DELETE "
                    + "FROM Pool WHERE (status=? OR status=?) AND operation=?");

            ps.setString(1, Operation.Status.Done.name());
            ps.setString(2, Operation.Status.Failed.name());
            ps.setString(3, Operation.Type.Delete.name());
            ps.execute();
            ps.close();

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public void removeOperationBySourceAndType(String source, Operation.Type type) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("DELETE "
                    + "FROM Pool WHERE source=? AND operation=?");

            ps.setString(1, source);
            ps.setString(2, type.name());
            ps.execute();
            ps.close();

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public void removeOperationByDestAndType(String dest, Operation.Type type) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("DELETE "
                    + "FROM Pool WHERE dest=? AND operation=?");

            ps.setString(1, dest);
            ps.setString(2, type.name());
            ps.execute();
            ps.close();

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public void removeOperationsByUser(String user) throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("DELETE "
                    + "FROM Pool WHERE username = ? AND status <> ?");

            ps.setString(1, user);
            ps.setString(2, Operation.Status.Running.name());
            ps.execute();
            ps.close();

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public List<Operation> getOperationsByLimitDateUser(String user, int limit, Date startDate) throws DAOException {
        try {
            List<Operation> operations = new ArrayList<Operation>();
            PreparedStatement ps = connection.prepareStatement(
                    getOperationSelect()
                    + "WHERE username = ? AND registration < ? AND operation <> ? "
                    + "ORDER BY registration DESC "
                    + "LIMIT 0," + limit);

            ps.setString(1, user);
            ps.setTimestamp(2, new Timestamp(startDate.getTime()));
            ps.setString(3, Operation.Type.Replicate.name()); //TODO temporary hack for the VIP portal

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                operations.add(getOperation(rs));
            }
            ps.close();
            return operations;

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    @Override
    public void resetOperations() throws DAOException {
        try {
            PreparedStatement ps = connection.prepareStatement("UPDATE Pool "
                    + "SET status = ? WHERE status = ?");
            ps.setString(1, Operation.Status.Queued.name());
            ps.setString(2, Operation.Status.Running.name());
            ps.executeUpdate();
            ps.close();

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }

    private String getOperationSelect() {

        return "SELECT id, registration, source, dest, operation, status, "
                + "username, proxy, retrycount, size FROM Pool ";
    }

    /**
     *
     * @param rs
     * @return
     * @throws SQLException
     */
    private Operation getOperation(ResultSet rs) throws SQLException {

        return new Operation(
                rs.getString("id"),
                new Date(rs.getTimestamp("registration").getTime()),
                rs.getString("source"),
                rs.getString("dest"),
                Type.valueOf(rs.getString("operation")),
                Status.valueOf(rs.getString("status")),
                rs.getString("username"),
                rs.getString("proxy"),
                rs.getInt("retrycount"),
                rs.getDouble("size"));
    }

    @Override
    public List<Operation> getOldOperations(Date date) throws DAOException {
        
        try {
            List<Operation> operations = new ArrayList<Operation>();
            PreparedStatement ps = connection.prepareStatement(
                    getOperationSelect()
                    + "WHERE registration < ? ORDER BY registration DESC");

            ps.setTimestamp(1, new Timestamp(date.getTime()));
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                operations.add(getOperation(rs));
            }
            ps.close();
            return operations;

        } catch (SQLException ex) {
            logger.error(ex);
            throw new DAOException(ex);
        }
    }
}