package pt.haslab.ekit;

import io.atomix.catalyst.concurrent.SingleThreadContext;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import org.junit.Assert;
import org.junit.Test;
import spread.MembershipInfo;
import spread.SpreadGroup;
import spread.SpreadMessage;

import java.util.concurrent.CompletableFuture;

public class TestSpread {
    @Test
    public void testSpread() throws Exception {
        ThreadContext tc = new SingleThreadContext("proto-%d", new Serializer());

        Spread sp = new Spread("testp", true);

        CompletableFuture<SpreadGroup> gf = new CompletableFuture<>();
        sp.handler(MembershipInfo.class, (m,g) -> {
            gf.complete(g.getGroup());
        });

        CompletableFuture<String> sf = new CompletableFuture<>();
        sp.handler(String.class, (m,s) -> {
            sf.complete(s);
        });

        tc.execute(() -> sp.open()).join().get();

        SpreadGroup g = tc.execute(() -> sp.join("testg")).join();

        Assert.assertEquals(g, gf.get());

        String msg = "Hello world!";
        tc.execute(() -> {
            SpreadMessage m = new SpreadMessage();
            m.addGroup(g);

            sp.multicast(m, msg);
        });

        Assert.assertEquals(msg, sf.get());

        sp.close();
    }
}
