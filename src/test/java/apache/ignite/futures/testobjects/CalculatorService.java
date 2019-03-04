package apache.ignite.futures.testobjects;

import apache.ignite.futures.topicmessage.TopicMessageFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Ignite Service implementation of {@link Calculator}.
 */
public class CalculatorService implements Calculator, Service {
    /** Ignite. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Indicates if the last operation was cancelled. */
    private AtomicBoolean wasCancelled = new AtomicBoolean(false);

    /**
     * {@inheritDoc}
     */
    @Override
    public TopicMessageFuture<Integer> sum(int n1, int n2, int duration, String failureMsg) {
        wasCancelled.set(false);
        TopicMessageFuture<Integer> fut = new TopicMessageFuture<Integer>()
            .setCancellation(() -> wasCancelled.set(true), 30_000)
            .setIgnite(ignite);

        if (duration > 0)
            CompletableFuture.runAsync(() -> {
                // The operation takes "duration" milliseconds
                try {
                    final int ITERATIONS_CNT = 10;

                    if (duration > ITERATIONS_CNT) {
                        for (int i = 0; i < ITERATIONS_CNT && !wasCancelled.get(); i++)
                            Thread.sleep(duration / ITERATIONS_CNT);
                    }
                    else
                        Thread.sleep(duration);
                }
                catch (InterruptedException ignored) {
                }

                if (!wasCancelled.get()) {
                    try {
                        if (failureMsg != null)
                            fut.fail(failureMsg, 30_000);
                        else
                            fut.resolve(n1 + n2, 30_000);
                    }
                    catch (Exception e) {
                        ignite.log().error(e.toString());
                    }
                }
            });
        else {
            try {
                if (failureMsg != null)
                    fut.fail(failureMsg, 30_000);
                else
                    fut.resolve(n1 + n2, 30_000); // synchronous computation
            }
            catch (InterruptedException ignored) {
            }
        }

        return fut;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean wasCancelled() {
        return wasCancelled.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cancel(ServiceContext svcCtx) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(ServiceContext svcCtx) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(ServiceContext svcCtx) {
    }
}
