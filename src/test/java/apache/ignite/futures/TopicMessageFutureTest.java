package apache.ignite.futures;

import apache.ignite.futures.testobjects.Calculator;
import apache.ignite.futures.testobjects.CalculatorService;
import apache.ignite.futures.testobjects.Cluster;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.junit.Test;

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
    public void testGetBeforeOperationCompletes() {
        try (Cluster cluster = new Cluster()) {
            IgniteFuture<Integer> calcFut = asyncSum(cluster.client(), 1, 2);

            int actual = calcFut.get();

            assertEquals(1 + 2, actual);
            assertTrue(calcFut.isDone());
        }
    }

    /**
     * Calling {@link TopicMessageFuture#get()} after the operation is complete.
     */
    @Test
    public void testGetAfterOperationCompletes() throws Exception {
        try (Cluster cluster = new Cluster()) {
            IgniteFuture<Integer> calcFut = asyncSum(cluster.client(), 1, 2);

            Thread.sleep(CalculatorService.OPERATION_DURATION + 500);

            int actual = calcFut.get();

            assertEquals(1 + 2, actual);
            assertTrue(calcFut.isDone());
        }
    }

    /**
     * {@link TopicMessageFuture#get(long, TimeUnit)} test.
     */
    @Test(expected = IgniteFutureTimeoutException.class)
    public void testGetTimeout() {
        try (Cluster cluster = new Cluster()) {
            IgniteFuture<Integer> calcFut = asyncSum(cluster.client(), 1, 2);

            calcFut.get(CalculatorService.OPERATION_DURATION / 4, TimeUnit.MILLISECONDS);

            assertTrue(calcFut.isDone());
        }
    }

    /**
     * {@link TopicMessageFuture#cancel()} test.
     */
    @Test
    public void testCancel() {
        try (Cluster cluster = new Cluster()) {
            IgniteFuture<Integer> calcFut = asyncSum(cluster.client(), 1, 2);

            boolean isCancelled = calcFut.cancel();

            assertTrue(isCancelled);
            assertTrue(calcFut.isCancelled());
        }
    }

    /**
     * Calling {@link TopicMessageFuture#cancel()} during {@link TopicMessageFuture#get()}.
     */
    @Test(expected = IgniteFutureCancelledException.class)
    public void testCancelGet() {
        try (Cluster cluster = new Cluster()) {
            IgniteFuture<Integer> calcFut = asyncSum(cluster.client(), 1, 2);

            Executors.newFixedThreadPool(1).submit(() -> {
                try {
                    Thread.sleep(CalculatorService.OPERATION_DURATION/4);

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
     * @return {@link Calculator} service proxy.
     */
    private static IgniteFuture<Integer> asyncSum(Ignite ignite, int a, int b) {
        return ignite.services().serviceProxy("Calculator", Calculator.class, false).sum(a, b).setIgnite(ignite);
    }
}
