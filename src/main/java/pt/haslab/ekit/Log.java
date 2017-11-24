package pt.haslab.ekit;

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.util.Managed;

import java.io.ByteArrayInputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

/**
 * A simple transactional log file. It is aimed at storing messages that
 * get replayed in the same order when the process restarts.
 */
public class Log implements Managed<Log> {

    private final String name;
    private final ExecutorService exec;
    private Connection connection;
    private PreparedStatement appendStmt;
    private PreparedStatement trimStmt;

    private ThreadContext tc;
    private final Map<Class<? extends Object>, BiConsumer<Integer, Object>> handlers;
    private CompletableFuture<Log> opened;
    private CompletableFuture<Void> closed;

    public Log(String name) {
        this.name = name;
        
        exec = Executors.newFixedThreadPool(1);
        handlers = new HashMap<>();
    }

    /**
     * Open log. This will trigger previously registered record
     * handlers for all existing log entries. No other method
     * can be used before this completes.
     *
     * @return the open log
     */
    @Override
    public CompletableFuture<Log> open() {
        if (opened != null)
            return opened;
        opened = new CompletableFuture<>();
        tc = ThreadContext.currentContextOrThrow();
        exec.execute(() -> {
            try {
                connection = DriverManager.getConnection("jdbc:derby:"+name+";create=true");

                try(Statement s = connection.createStatement()) {
                    s.executeUpdate("CREATE TABLE log (lsn INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), data VARCHAR (32672) FOR BIT DATA )");
                } catch (SQLException e) {
                    if (!e.getSQLState().equals("X0Y32")) // Already exists
                        throw e;
                }

                appendStmt = connection.prepareStatement("INSERT INTO log(data) VALUES (?)", PreparedStatement.RETURN_GENERATED_KEYS);
                trimStmt = connection.prepareStatement("DELETE FROM log WHERE lsn < ?");

            } catch(SQLException e) {
                opened.completeExceptionally(e);
                return;
            }

            try(Statement s = connection.createStatement();
                ResultSet rs = s.executeQuery("SELECT * FROM log ORDER BY lsn")) {
                while(rs.next()) {
                    final int lsn = rs.getInt(1);
                    final byte[] data = rs.getBytes(2);

                    tc.execute(() -> {
                        Object record = tc.serializer().readObject(new ByteArrayInputStream(data));

                        handlers.get(record.getClass()).accept(lsn, record);

                    }).join();
                }

                opened.complete(this);

            } catch(SQLException e) {
                opened.completeExceptionally(e);
            }
        });
        return opened;
    }

    @Override
    public boolean isOpen() {
        return opened != null && opened.isDone();
    }

    @Override
    public CompletableFuture<Void> close() {
        if (closed != null)
            return closed;
        closed = new CompletableFuture<>();
        exec.execute(() -> {
            try {
                connection.close();
                closed.complete(null);
            } catch(SQLException e) {
                closed.completeExceptionally(e);
            }
        });
        exec.shutdown();
        return closed;
    }

    @Override
    public boolean isClosed() {
        return closed != null;
    }

    /**
     * Add a record handler. The handler is used for recovery, when the log is
     * not empty when opened.
     *
     * @param type record class
     * @param rh record handler
     * @param <T> record type
     * @return the log itself for chaining invocations
     */
    public <T> Log handler(Class<T> type, BiConsumer<Integer, T> rh) {
        handlers.put(type, (i,r) ->  rh.accept(i, type.cast(r)) );
        return this;
    }

    /**
     * Append one record to the log. This can only be used after
     * the log is open and recovered.
     *
     * @param record new log record
     * @return index of new record in log
     */
    public <T> CompletableFuture<Integer> append(T record) {
        Buffer buffer = tc.serializer().writeObject(record);
        buffer.flip();
        byte[] data = new byte[(int) buffer.remaining()];
        buffer.read(data);

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