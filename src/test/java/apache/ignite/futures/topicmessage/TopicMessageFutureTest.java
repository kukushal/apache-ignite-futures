package apache.ignite.futures.topicmessage;

import apache.ignite.futures.testobjects.Calculator;
import apache.ignite.futures.testobjects.Cluster;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link TopicMessageFuture} integration tests.
 */
public class TopicMessageFutureTest {
    /**
     * Calling {@link TopicMessageFuture#get()} before the operation is complete.
     */
    @Test
    public void getResultBeforeOperationCompletes() {
        try (Cluster cluster = new Cluster()) {
            IgniteFuture<Integer> calcFut = asyncSum(cluster.client(), 1, 2, 2000);

            int actual = calcFut.get();

            assertEquals(1 + 2, actual);
            assertTrue(calcFut.isDone());
        }
    }

    /**
     * Calling {@link TopicMessageFuture#get()} after the operation is complete.
     */
    @Test
    public void getResultAfterOperationCompletes() throws Exception {
        try (Cluster cluster = new Cluster()) {
            IgniteFuture<Integer> calcFut = asyncSum(cluster.client(), 1, 2, 10);

            Thread.sleep(1000);

            int actual = calcFut.get();

            assertEquals(1 + 2, actual);
            assertTrue(calcFut.isDone());
        }
    }

    /**
     * Calling {@link TopicMessageFuture#get()} for the synchronous operation.
     */
    @Test
    public void getSynchronousOperationResult() {
        try (Cluster cluster = new Cluster()) {
            IgniteFuture<Integer> calcFut = asyncSum(cluster.client(), 1, 2, 0 /* 0 means sync execution */);

            int actual = calcFut.get();

            assertEquals(1 + 2, actual);
            assertTrue(calcFut.isDone());
        }
    }

    /**
     * {@link TopicMessageFuture#get(long, TimeUnit)} test.
     */
    @Test(expected = IgniteFutureTimeoutException.class)
    public void getResultTimesOut() {
        try (Cluster cluster = new Cluster()) {
            IgniteFuture<Integer> calcFut = asyncSum(cluster.client(), 1, 2, 2000);

            calcFut.get(2000 / 4, TimeUnit.MILLISECONDS);

            assertTrue(calcFut.isDone());
        }
    }

    /**
     * {@link TopicMessageFuture#cancel()} test.
     */
    @Test
    public void cancelOperationFromSameClient() {
        try (Cluster cluster = new Cluster()) {
            IgniteFuture<Integer> calcFut = asyncSum(cluster.client(), 1, 2, 20000);

            boolean isCancelled = calcFut.cancel();

            assertTrue(isCancelled);
            assertTrue(calcFut.isCancelled());
            assertTrue(serviceProxy(cluster.client()).wasCancelled());
        }
    }

    /**
     * Calling {@link TopicMessageFuture#cancel()} during {@link TopicMessageFuture#get()}.
     */
    @Test(expected = IgniteFutureCancelledException.class)
    public void cancelOperationWhileClientWaitsForResult() {
        try (Cluster cluster = new Cluster()) {
            IgniteFuture<Integer> calcFut = asyncSum(cluster.client(), 1, 2, 2000);

            Executors.newFixedThreadPool(1).submit(() -> {
                try {
                    Thread.sleep(2000 / 4);

                    calcFut.cancel();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            calcFut.get();
        }
    }

    /**
     * {@link TopicMessageFuture#listen(IgniteInClosure)} test.
     */
    @Test
    public void listenForResultBeforeOperationCompletes() throws InterruptedException {
        try (Cluster cluster = new Cluster()) {
            IgniteFuture<Integer> calcFut = asyncSum(cluster.client(), 1, 2, 1000);

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

    /**
     * {@link TopicMessageFuture#chain(IgniteClosure)} test.
     */
    @Test
    public void chainResultBeforeOperationCompletes() {
        try (Cluster cluster = new Cluster()) {
            IgniteFuture<Integer> calcFut = asyncSum(cluster.client(), 1, 2, 1000);

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
