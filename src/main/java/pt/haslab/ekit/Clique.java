package pt.haslab.ekit;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.concurrent.Futures;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.Listeners;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.TypeSerializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * A simple fully connected network.
 */
public class Clique implements Managed<Clique> {
    private final Logger logger = LoggerFactory.getLogger(Clique.class);

    private final Transport transport;
    private final int me;
    private final Address[] addresses;

    private final Connection[] connections;
    private Connection loopback;
    private final List<BiConsumer<int[],Connection>> msgHandlers;
    private final Listeners<Clique> closeHandlers;
    private final Listeners<Throwable> exceptionHandlers;
    private Server server;
    private int n;
    private CompletableFuture<Clique> opened;
    private CompletableFuture<Void> closed;

    /**
     * Create a new clique.
     *
     * @param transport Catalyst trnasport
     * @param me index of local process
     * @param addresses addresses of all processes
     */
    public Clique(Transport transport, int me, Address... addresses) {
        this.me = me;
        this.addresses = addresses;
        this.transport = transport;

        this.connections = new Connection[addresses.length];
        this.msgHandlers = new ArrayList<>();
        this.closeHandlers = new Listeners<>();
        this.exceptionHandlers = new Listeners<>();
    }

    /**
     * Connect the network. Note that messages may be delivered before
     * the operation is complete, as there is no way to make the setup
     * of the network atomic. Handlers should thus be registered before
     * calling this method, or otherwise messages can be dropped.
     *
     * @return a future connected clique
     */
    @Override
    public CompletableFuture<Clique> open() {
        if (opened !=null)
            return opened;

        ThreadContext.currentContext().serializer().register(Id.class, c->new IdSerializer());
        opened = new CompletableFuture<Clique>();
        this.server = transport.server();
        server.listen(addresses[me], c -> {
            final int[] i = new int[]{-1};
            handlers(i, c);
            c.handler(Id.class, id -> {
                logger.trace("process {}: connection from process {}", me, id.index);
                i[0] = id.index;
                connected(id.index, c);
            });
        }).thenRun(() -> {
            logger.debug("process {}: listening at {}", me, addresses[me]);
            for(int i=0; i<=me; i++)
                connect(i);
        });
        return opened;
    }

    /**
     * Add a message handler.
     *
     * @param type class of message to handle
     * @param mh a source process index and message consumer
     * @param <T> type of message
     * @return a clique, to chain ivocations
     */
    public <T> Clique handler(Class<T> type, BiConsumer<Integer,T> mh) {
        BiConsumer<int[],Connection> h = (i,c) -> {
            c.handler(type, m -> {
                mh.accept(i[0], m);
            });
        };
        for(int i = 0; i<connections.length; i++) {
            if (connections[i] != null)
                h.accept(new int[]{i}, connections[i]);
        }
        if (loopback != null)
            h.accept(new int[]{me}, loopback);
        msgHandlers.add(h);
        return this;
    }

    /**
     * Add a request handler.
     *
     * @param type class of request to handle
     * @param mh a source process index and message consumer
     * @param <T> type of request
     * @param <U> tupe of reply
     * @return a clique, to chain ivocations
     */
    public <T,U> Clique handler(Class<T> type, BiFunction<Integer,T,CompletableFuture<U>> mh) {
        BiConsumer<int[],Connection> h = (i,c) -> {
            c.handler(type, m -> {
                return mh.apply(i[0], m);
            });
        };
        for(int i = 0; i<connections.length; i++) {
            if (connections[i] != null)
                h.accept(new int[]{i}, connections[i]);
        }
        if (loopback != null)
            h.accept(new int[]{me}, loopback);
        msgHandlers.add(h);
        return this;
    }

    /**
     * Send a message.
     *
     * @param i index of destination
     * @param message message object
     * @return a future to wait on message being sent
     */
    public CompletableFuture<Void> send(int i, Object message) {
        if (connections[i] != null)
            return connections[i].send(message);
        else
            return Futures.exceptionalFuture(new IOException("connection lost"));
    }

    /**
     * Send a request.
     *
     * @param i index of destination process
     * @param message request object
     * @param <T> type of request
     * @param <U> type of reply
     * @return a future of the invocation reply
     */
    public <T,U> CompletableFuture<U> sendAndReceive(int i, T message) {
        if (connections[i] != null)
            return connections[i].sendAndReceive(message);
        else
            return Futures.exceptionalFuture(new IOException("connection lost"));
    }

    public Listener<Clique> onClose(Consumer<Clique> listener) {
        if (isClosed())
            listener.accept(this);
        return closeHandlers.add(listener);
    }

    public Listener<Throwable> onException(Consumer<Throwable> listener) {
        return exceptionHandlers.add(listener);
    }

    @Override
    public boolean isOpen() {
        return opened !=null && opened.isDone();
    }

    @Override
    public CompletableFuture<Void> close() {
        if (closed != null)
            return closed;
        closed = new CompletableFuture<Void>();
        server.close().thenRun(() -> {
            for(int i = 0; i< connections.length; i++) {
                final int peer = i;
                if (connections[i] != null) {
                    connections[i].close().thenRun(() -> {
                        closed(peer);
                    });
                    connections[i] = null;
                }
                if (loopback != null) {
                    loopback.close();
                    loopback = null;
                }
            }
        });
        return closed;
    }

    @Override
    public boolean isClosed() {
        return closed !=null && closed.isDone();
    }

    private void connect(int i) {
        logger.trace("process {}: connecting to process {} at {}", me, i, addresses[i]);
        transport.client().connect(addresses[i]).thenAccept(c -> {
            handlers(new int[]{i}, c);
            c.send(new Id(me)).thenRun(()->{
                connected(i, c);
            }).exceptionally(t->{ disconnected(i); return null; });
        }).exceptionally(t -> {
            logger.trace("process {}: retrying {}", me, i);
            ThreadContext.currentContext().schedule(Duration.ofSeconds(1), () -> {
                connect(i);
            });
            return null;
        });
    }

    private void connected(final int i, Connection c) {
        if (connections[i] == null) {
            logger.debug("process {}: connected to {}", me, i);
            connections[i] = c;
            n++;
        } else {
            loopback = c;
        }
        if (n == addresses.length && loopback != null) {
            logger.info("process {}: open", me);
            opened.complete(this);
        }
    }

    private void handlers(final int[] i, Connection c) {
        for(BiConsumer<int[],Connection> h: msgHandlers)
            h.accept(i, c);
        c.onClose(v -> { disconnected(i[0]); });
        c.onException(t -> { report(i[0],t); });
    }

    private void disconnected(int i) {
        logger.trace("process {}: disconnected from {}", me, i);
        close();
    }

    private void report(int i, Throwable t) {
        for(Listener<Throwable> l: exceptionHandlers)
            l.accept(t);
    }

    private void closed(int peer) {
        if (--n == 0) {
            for(Listener<Clique> l: closeHandlers)
                l.accept(this);
            closed.complete(null);
            logger.info("process {}: closed", me);
        }
    }

    private static class Id {
        public int index;

        public Id(int index) {
            this.index = index;
        }
    }

    private static class IdSerializer implements TypeSerializer<Id> {
        @Override
        public void write(Id id, BufferOutput bufferOutput, Serializer serializer) {
            bufferOutput.writeInt(id.index);
        }

        @Override
        public Id read(Class<Id> aClass, BufferInput bufferInput, Serializer serializer) {
            return new Id(bufferInput.readInt());
        }
    }
}