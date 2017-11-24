package pt.haslab.ekit;

import io.atomix.catalyst.concurrent.SingleThreadContext;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.transport.local.LocalServerRegistry;
import io.atomix.catalyst.transport.local.LocalTransport;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

public class TestClique {

    @Test
    public void ring() throws InterruptedException {

        int[] total = new int[]{0};

        Transport t = new LocalTransport(new LocalServerRegistry());
        ThreadContext tc = new SingleThreadContext("proto-%d", new Serializer());

        Address[] addresses = new Address[]{
                new Address("127.0.0.1:12345"),
                new Address("127.0.0.1:12346"),
                new Address("127.0.0.1:12347"),
                new Address("127.0.0.1:12348"),
                new Address("127.0.0.1:12349")
        };

        for(int i=0; i<addresses.length; i++) {
            final int id = i;
            tc.execute(() -> {
                Clique c = new Clique(t, Clique.Mode.ALL, id, addresses);

                c.handler(Integer.class, (j,m)-> {
                    Assert.assertEquals(j, m);
                    Assert.assertEquals((j+1)%addresses.length, id);
                    total[0]++;
                }).onException(e->{
                    e.printStackTrace();
                    Assert.fail(e.getMessage());
                });

                c.open().thenRun(() -> {
                    c.send((id+1)%addresses.length, id);
                });
            }).join();
        }

        Thread.sleep(1000);
        tc.close();

        Assert.assertEquals(addresses.length, total[0]);
    }

    @Test
    public void majority() throws InterruptedException {

        int[] total = new int[]{0};

        Transport t = new LocalTransport(new LocalServerRegistry());
        ThreadContext tc = new SingleThreadContext("proto-%d", new Serializer());

        Address[] addresses = new Address[]{
                new Address("127.0.0.1:12345"),
                new Address("127.0.0.1:12346"),
                new Address("127.0.0.1:12347"),
        };

        for(int i=0; i<addresses.length; i++) {
            final int id = i;
            tc.execute(() -> {
                Clique c = new Clique(t, Clique.Mode.MAJORITY, id, addresses);
                c.open().thenRun(() -> {
                    total[0] ++;
                });
            }).join();

            if (i < addresses.length+1/2)
                Assert.assertEquals(0, total[0]);
        }

        Thread.sleep(1000);
        tc.close();

        Assert.assertEquals(addresses.length, total[0]);
    }

    @Test
    public void reconnect() throws InterruptedException {

        int[] total = new int[]{0};

        Transport t = new LocalTransport(new LocalServerRegistry());
        ThreadContext tc = new SingleThreadContext("proto-%d", new Serializer());

        Address[] addresses = new Address[]{
                new Address("127.0.0.1:12345"),
                new Address("127.0.0.1:12346"),
                new Address("127.0.0.1:12347"),
        };

        tc.execute(() -> {
            Clique c = new Clique(t, Clique.Mode.ANY, 1, addresses);
            c.open().thenRun(() -> {
                c.handler(Integer.class, (j,m) -> {
                    total[0] ++;
                });
            });
        }).join();

        tc.execute(() -> {
            Clique c = new Clique(t, Clique.Mode.ANY, 0, addresses);
            c.open().thenRun(() -> {
                tc.schedule(Duration.ofSeconds(2), ()-> {
                    c.send(1, 123).thenRun(() -> {
                        c.close();
                    });
                });
            });
        }).join();

        Thread.sleep(4000);
        Assert.assertEquals(1, total[0]);

        tc.execute(() -> {
            Clique c = new Clique(t, Clique.Mode.ANY, 2, addresses);
            c.open().thenRun(() -> {
                tc.schedule(Duration.ofSeconds(2), ()-> {
                    c.send(1, 123).thenRun(() -> {
                        c.close();
                    });
                });
            });
        }).join();

        Thread.sleep(4000);
        Assert.assertEquals(2, total[0]);

        tc.execute(() -> {
            Clique c = new Clique(t, Clique.Mode.ANY, 0, addresses);
            c.open().thenRun(() -> {
                tc.schedule(Duration.ofSeconds(2), ()-> {
                    c.send(1, 123).thenRun(() -> {
                        c.close();
                    });
                });
            });
        }).join();

        Thread.sleep(4000);
        Assert.assertEquals(3, total[0]);

        tc.execute(() -> {
            Clique c = new Clique(t, Clique.Mode.ANY, 2, addresses);
            c.open().thenRun(() -> {
                tc.schedule(Duration.ofSeconds(2), ()-> {
                    c.send(1, 123).thenRun(() -> {
                        c.close();
                    });
                });
            });
        }).join();

        Thread.sleep(4000);
        Assert.assertEquals(4, total[0]);

        tc.close();
    }
}
