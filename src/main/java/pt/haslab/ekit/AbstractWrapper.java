package pt.haslab.ekit;

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.util.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

/**
 * Base class for event-driven IO wrappers. It handles life-cycle, serialization,
 * handlers and dispatching.
 *
 * @param <D> derived concrete class
 * @param <M> meta-data class
 */
public abstract class AbstractWrapper<D,M> implements Managed<D> {

    private final Logger logger = LoggerFactory.getLogger(AbstractWrapper.class);

    protected final ExecutorService exec;
    protected ThreadContext tc;

    private final Map<Class<? extends Object>, BiConsumer<M, Object>> handlers;
    private CompletableFuture<D> opened;
    private CompletableFuture<Void> closed;

    public AbstractWrapper() {
        exec = Executors.newFixedThreadPool(1);
        handlers = new HashMap<>();
    }

    /**
     * Start this wrapper. No method other than {@link #open()} can be used before this completes.
     *
     * @return the wrapper itself
     */
    @Override
    public CompletableFuture<D> open() {
        if (opened != null)
            return opened;
        opened = new CompletableFuture<>();
        tc = ThreadContext.currentContextOrThrow();
        exec.execute(() -> {
            try {
                doOpen();
                opened.complete((D)this);
            } catch(Exception e) {
                opened.completeExceptionally(e);
            }
        });
        return opened;
    }

    protected abstract void doOpen() throws Exception;

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
                doClose();
                closed.complete(null);
            } catch(Exception e) {
                closed.completeExceptionally(e);
            }
        });
        exec.shutdown();
        return closed;
    }

    protected abstract void doClose() throws Exception;

    @Override
    public boolean isClosed() {
        return closed != null;
    }

    /**
     * Add handler. To avoid races, this should be done before calling {@link #open()}.
     *
     * @param type data class
     * @param rh data handler
     * @param <T> data type
     * @return the wrapper itself for chaining
     */
    public <T> D handler(Class<T> type, BiConsumer<M, T> rh) {
        handlers.put(type, (i,r) ->  rh.accept(i, type.cast(r)) );
        return (D) this;
    }

    /**
     * Seriaize data object. Call from event-handler thread.
     *
     * @param object data object
     * @return serialized data
     */
    protected byte[] serialize(Object object) {
        Buffer buffer = tc.serializer().writeObject(object);
        buffer.flip();
        byte[] data = new byte[(int) buffer.remaining()];
        buffer.read(data);
        return data;
    }

    /**
     * Deserialize and dispatch data. Call outside the event-handler thread.
     *
     * @param meta meta-data object
     * @param data data serialized object
     */
    protected void dispatch(M meta, byte[] data) {
        tc.execute(() -> {
            callHandler(meta, tc.serializer().readObject(new ByteArrayInputStream(data)));
        }).join();
    }

    /**
     * Dispatch data. Call outside the event-handler thread.
     *
     * @param meta meta-data object
     * @param object data object
     */
    protected void dispatch(M meta, Object object) {
        tc.execute(() -> {
            callHandler(meta, object);
        }).join();
    }

    private void callHandler(M meta, Object object) {
        BiConsumer<M, Object> handler = handlers.get(object.getClass());

        if (handler == null) {
            logger.error("no handler for class {}, ignoring {}", object.getClass().getName(), meta);
        } else {
            handler.accept(meta, object);
        }
    }
}