package pt.haslab.ekit;

import io.atomix.catalyst.concurrent.SingleThreadContext;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.atomic.AtomicInteger;

public class TestLog {

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    @Test
    public void testLog() throws Exception {
        ThreadContext tc = new SingleThreadContext("proto-%d", new Serializer());
        String path = tf.newFolder().getAbsolutePath()+"/test";

        Log log1 = new Log(path);
        tc.execute(() -> log1.open()).join().get();

        for(int i=0; i<10; i++) {
            final String data = "test"+(i+1);

            int r = tc.execute(() -> log1.append(data)).join().get();

            Assert.assertEquals(i+1, r);
        }

        tc.execute(() -> log1.close()).join().get();

        final AtomicInteger last =  new AtomicInteger(0);

        Log log2 = new Log(path);
        log2.handler(String.class, (i,m) -> {
            Assert.assertEquals("test"+i, m);
            Assert.assertEquals(last.incrementAndGet(), i.intValue());
        });
        tc.execute(() -> log2.open()).join().get();

        Assert.assertEquals(last.get(), 10);

        tc.execute(() -> log2.close()).join().get();

    }

    @Test
    public void testPrune() throws Exception {
        ThreadContext tc = new SingleThreadContext("proto-%d", new Serializer());
        String path = tf.newFolder().getAbsolutePath()+"/test";

        Log log1 = new Log(path);
        tc.execute(() -> log1.open()).join().get();

        for(int i=0; i<10; i++) {
            final String data = "test"+(i+1);

            int r = tc.execute(() -> log1.append(data)).join().get();

            Assert.assertEquals(i+1, r);
        }

        int r = tc.execute(() -> log1.trim(5)).join().get();

        Assert.assertEquals(4, r);

        tc.execute(() -> log1.close()).join().get();

        final AtomicInteger last =  new AtomicInteger(4);

        Log log2 = new Log(path);
        log2.handler(String.class, (i,m) -> {
            Assert.assertEquals("test"+i, m);
            Assert.assertEquals(last.incrementAndGet(), i.intValue());
        });
        tc.execute(() -> log2.open()).join().get();

        Assert.assertEquals(last.get(), 10);

        tc.execute(() -> log2.close()).join().get();
    }

    @Test
    public void testMissingHandler() throws Exception {
        ThreadContext tc = new SingleThreadContext("proto-%d", new Serializer());
        String path = tf.newFolder().getAbsolutePath() + "/test";

        Log log1 = new Log(path);
        tc.execute(() -> log1.open()).join().get();

        int r = tc.execute(() -> log1.append("a string")).join().get();

        log1.close();

        Log log2 = new Log(path);
        tc.execute(() -> log2.open()).join().get();
        log2.close();

    }
}
