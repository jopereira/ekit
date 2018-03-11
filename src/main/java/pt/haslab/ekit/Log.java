package pt.haslab.ekit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.CompletableFuture;

/**
 * A simple transactional log file. It is aimed at storing messages that
 * get replayed in the same order when the process restarts. Calling {@link #open()}
 * will trigger previously registered record handlers for all existing log entries.
 */
public class Log extends AbstractWrapper<Log,Integer> {

    private final Logger logger = LoggerFactory.getLogger(Log.class);

    private final String name;
    private Connection connection;
    private PreparedStatement appendStmt;
    private PreparedStatement trimStmt;

    public Log(String name) {
        this.name = name;
    }

    @Override
    protected void doOpen() throws Exception {
        connection = DriverManager.getConnection("jdbc:derby:"+name+";create=true");

        try(Statement s = connection.createStatement()) {
            s.executeUpdate("CREATE TABLE log (lsn INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), data VARCHAR (32672) FOR BIT DATA )");
        } catch (SQLException e) {
            if (!e.getSQLState().equals("X0Y32")) // Already exists
                throw e;
        }

        appendStmt = connection.prepareStatement("INSERT INTO log(data) VALUES (?)", PreparedStatement.RETURN_GENERATED_KEYS);
        trimStmt = connection.prepareStatement("DELETE FROM log WHERE lsn < ?");

        try(Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery("SELECT * FROM log ORDER BY lsn")) {
            while (rs.next()) {
                final int lsn = rs.getInt(1);
                final byte[] data = rs.getBytes(2);

                dispatch(lsn, data);
            }
        } catch(SQLException e) {
            throw e;
        }
    }

    @Override
    protected void doClose() throws Exception {
        connection.close();
    }

    /**
     * Append one record to the log. This can only be used after
     * the log is open and recovered.
     *
     * @param record new log record
     * @return index of new record in log
     */
    public <T> CompletableFuture<Integer> append(T record) {
        byte[] data = serialize(record);

        final CompletableFuture<Integer> result = new CompletableFuture<>();
        exec.execute(() -> {
            try {

                appendStmt.setBytes(1, data);
                appendStmt.executeUpdate();

                ResultSet rs = appendStmt.getGeneratedKeys();
                if (rs.next())
                    result.complete(rs.getInt(1));
                else
                    result.completeExceptionally(new SQLException());

            } catch(SQLException e) {
                result.completeExceptionally(e);
            }
        });
        return result;
    }

    /**
     * Delete all records before n, if any. Record n itself is kept. This
     * should be used during normal operation, when it is known
     * that a prefix of the log is no longer needed.
     *
     * @param n number of first record to keep
     * @return number of records deleted
     */
    public CompletableFuture<Integer> trim(int n) {
        final CompletableFuture<Integer> result = new CompletableFuture<>();
        exec.execute(() -> {
            try {
                trimStmt.setInt(1, n);
                int c = trimStmt.executeUpdate();
                result.complete(c);
            } catch(SQLException e) {
                result.completeExceptionally(e);
            }
        });
        return result;
    }
}