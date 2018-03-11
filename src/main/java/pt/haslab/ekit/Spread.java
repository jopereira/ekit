package pt.haslab.ekit;

import io.atomix.catalyst.transport.Address;
import spread.*;

import java.util.function.BiConsumer;

/**
 * Simple wrapper for Spread group communication toolkit. View change notifications
 * are received by registering a handler for {@link spread.MembershipInfo}.
 * Handlers for messages and views
 * should be registerd with {@link #handler(Class, BiConsumer)} before the
 * connection is opened by calling {@link #open()}. All methods need to be
 * called from the event handling thread.
 */
public class Spread extends AbstractWrapper<Spread, SpreadMessage> {
    private final Address address;
    private final String privateName;
    private final boolean groupMembership;
    private SpreadConnection conn;

    /**
     * Create a connection to the Spread toolkit on localhost:4803.
     *
     * @param privateName unique name of this process
     * @param groupMembership true to receive view changes
     * @throws SpreadException
     */
    public Spread(String privateName, boolean groupMembership) throws SpreadException {
        this(null, privateName, groupMembership);
    }

    /**
     * Create a connection to the Spread toolkit.
     *
     * @param address address of the Spread daemon (usually, "localhost:4803")
     * @param privateName unique name of this process
     * @param groupMembership true to receive view changes
     * @throws SpreadException
     */
    public Spread(Address address, String privateName, boolean groupMembership) throws SpreadException {
        conn = new SpreadConnection();

        if (address != null)
            this.address = address;
        else
            this.address = new Address("localhost", 4803);
        this.privateName = privateName;
        this.groupMembership = groupMembership;
    }

    @Override
    protected void doOpen() throws Exception {
        conn.connect(address.socketAddress().getAddress(), address.port(), privateName, false, groupMembership);

        conn.add(new AdvancedMessageListener() {
            @Override
            public void regularMessageReceived(SpreadMessage message) {
                dispatch(message, message.getData());
            }

            @Override
            public void membershipMessageReceived(SpreadMessage message) {
                dispatch(message, message.getMembershipInfo());
            }
        });
    }

    @Override
    protected void doClose() throws Exception {
        conn.disconnect();
    }

    /**
     * Join a named group. Requires that the connection is open. This is an asyncrhonous
     * operation that is confirmed by receving the first view change notification.
     *
     * @param name name of the group
     * @return group identifier
     */
    public SpreadGroup join(String name) {
        SpreadGroup group = new SpreadGroup();
        exec.submit(() -> {
            try {
                group.join(conn, name);
            } catch (SpreadException e) {
                e.printStackTrace();
            }
        });
        return group;
    }

    /**
     * Leave current group. This is an asyncrhonous operation that is confirmed by
     * receving the first view change notification.
     *
     * @param group group identifier
     */
    public void leave(SpreadGroup group) {
        exec.submit(() -> {
            try {
                group.leave();
            } catch (SpreadException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Multicast a message. The destination and QoS is set in the {@link SpreadMessage}
     * envelope. The message is delivered to the corresponding handler at the
     * destinations.
     *
     * @param envelope an envelope for the message with destination and QoS
     * @param object an object to be serialized and sent as the payload
     * @param <T>
     */
    public <T> void multicast(SpreadMessage envelope, T object) {
        if (object != null) {
            byte[] data = serialize(object);
            envelope.setData(data);
        }
        exec.submit(() -> {
            try {
                conn.multicast(envelope);
            } catch (SpreadException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Returns the name of the private group: a group containing only the local
     * process.
     *
     * @return the name of the local process
     */
    public SpreadGroup getPrivateGroup() {
        return conn.getPrivateGroup();
    }
}
