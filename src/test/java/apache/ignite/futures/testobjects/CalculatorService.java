package apache.ignite.futures.testobjects;

import apache.ignite.futures.TopicMessageFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Ignite Service implementation of {@link Calculator}.
 */
public class CalculatorService implements Calculator, Service {
    /**
     * Operation duration in milliseconds.
     */
    public static final int OPERATION_DURATION = 2000;

    /** Ignite. */
    @IgniteInstanceResource
    private Ignite ignite;

    /**
     * {@inheritDoc}
     */
    @Override
    public TopicMessageFuture<Integer> sum(int n1, int n2) {
        AtomicBoolean isCancelled = new AtomicBoolean(false);
        TopicMessageFuture<Integer> fut = new TopicMessageFuture<Integer>()
            .setCancellation(() -> isCancelled.set(true))
            .setIgnite(ignite);

        ignite.compute().runAsync(() -> {
            // The operation takes OPERATION_DURATION milliseconds
            try {
                for (int i = 0; i < 10 && !isCancelled.get(); i++)
                    Thread.sleep(OPERATION_DURATION/10);
            }
            catch (InterruptedException ignored) {
            }

            if (!isCancelled.get()) {
                try {
                    fut.resolve(n1 + n2);
                }
                catch (Exception e) {
                    ignite.log().error(e.toString());
                }
            }
        });

        return fut;
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
