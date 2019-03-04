package apache.ignite.futures;

import apache.ignite.futures.testobjects.Calculator;
import apache.ignite.futures.testobjects.DotNetServer;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Java client calls Ignite.NET service.
 */
public class JavaCallingDotNetTests {
    @Test
    public void getResultBeforeOperationCompletes() throws Exception {
        try (DotNetServer ignored = new DotNetServer(); Ignite ignite = Ignition.start("ignite-client.xml")) {
            IgniteFuture<Integer> calcFut = asyncSum(ignite, 1, 2, 2000);

            int actual = calcFut.get();

            assertEquals(1 + 2, actual);
            assertTrue(calcFut.isDone());
        }
    }

    @Test
    public void getResultAfterOperationCompletes() throws Exception {
        try (DotNetServer ignored = new DotNetServer(); Ignite ignite = Ignition.start("ignite-client.xml")) {
            IgniteFuture<Integer> calcFut = asyncSum(ignite, 1, 2, 10);

            Thread.sleep(1_000);

            int actual = calcFut.get();

            assertEquals(1 + 2, actual);
            assertTrue(calcFut.isDone());
        }
    }

    @Test
    public void getSynchronousOperationResult() throws Exception {
        try (DotNetServer ignored = new DotNetServer(); Ignite ignite = Ignition.start("ignite-client.xml")) {
            IgniteFuture<Integer> calcFut = asyncSum(ignite, 1, 2, 0 /* 0 means sync execution */);

            assertTrue(calcFut.isDone());

            int actual = calcFut.get();

            assertEquals(1 + 2, actual);
        }
    }

    @Test(expected = IgniteFutureTimeoutException.class)
    public void getResultTimesOut() throws Exception {
        try (DotNetServer ignored = new DotNetServer(); Ignite ignite = Ignition.start("ignite-client.xml")) {
            IgniteFuture<Integer> calcFut = asyncSum(ignite, 1, 2, 20_000);

            calcFut.get(1_000, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void cancelOperationFromSameClient() throws Exception {
        try (DotNetServer ignored = new DotNetServer(); Ignite ignite = Ignition.start("ignite-client.xml")) {
            IgniteFuture<Integer> calcFut = asyncSum(ignite, 1, 2, 60_000);

            boolean isCancelled = calcFut.cancel();

            assertTrue(isCancelled);
            assertTrue(calcFut.isCancelled());
        }
    }

    @Test(expected = IgniteFutureCancelledException.class)
    public void cancelOperationWhileClientWaitsForResult() throws Exception {
        try (DotNetServer ignored = new DotNetServer(); Ignite ignite = Ignition.start("ignite-client.xml")) {
            IgniteFuture<Integer> calcFut = asyncSum(ignite, 1, 2, 60_000);

            Executors.newFixedThreadPool(1).submit(() -> {
                try {
                    Thread.sleep(1_000);

                    calcFut.cancel();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            calcFut.get();
        }
    }

    @Test
    public void listenForResultBeforeOperationCompletes() throws Exception {
        try (DotNetServer ignored = new DotNetServer(); Ignite ignite = Ignition.start("ignite-client.xml")) {
            IgniteFuture<Integer> calcFut = asyncSum(ignite, 1, 2, 1000);

            CountDownLatch latch = new CountDownLatch(1);

            calcFut.listen(fut -> {
                assertTrue(fut.isDone());

                int actual = fut.get();

                assertEquals(1 + 2, actual);

                latch.countDown();
            });

            latch.await(1000 * 2, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void chainResultBeforeOperationCompletes() throws Exception {
        try (DotNetServer ignored = new DotNetServer(); Ignite ignite = Ignition.start("ignite-client.xml")) {
            IgniteFuture<Integer> calcFut = asyncSum(ignite, 1, 2, 1000);

            IgniteFuture<String> chainedFut = calcFut.chain(fut -> fut.get().toString());

            String actual = chainedFut.get();

            assertEquals(Integer.valueOf(1 + 2).toString(), actual);
            assertTrue(chainedFut.isDone());
        }
    }

    /**
     * @return {@link IgniteFuture} from {@link Calculator#sum(int, int, int)}.
     */
    private static IgniteFuture<Integer> asyncSum(Ignite ignite, int a, int b, int duration) {
        return serviceProxy(ignite).sum(a, b, duration).setIgnite(ignite);
    }

    /**
     * @return {@link Calculator} non-sticky service proxy.
     */
    private static Calculator serviceProxy(Ignite ignite) {
        return ignite.services().serviceProxy("Calculator", Calculator.class, false);
    }
}
