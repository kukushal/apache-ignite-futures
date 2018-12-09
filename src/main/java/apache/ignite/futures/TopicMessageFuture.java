package apache.ignite.futures;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Apache Ignite implementation of {@link IgniteFuture} based on
 * <a href="https://apacheignite.readme.io/docs/messaging">Ignite Messaging</a>.
 * <p>
 * The future consist of separate client and server sides with separate APIs:
 * <ul>
 *     <li>The server creates the future and returns it to the client.</li>
 *     <li>
 *         The server starts asynchronous operation and calls {@link #resolve(Object, long)} when the operation
 *         is complete.
 *     </li>
 *     <li>
 *         The client calls {@link #get()} to synchronously wait for the operation result or
 *         {@link #chain}/{@link #listen(IgniteInClosure)} to asynchronously receive result of the operation.
 *     </li>
 *     <li>
 *         The asynchronous operation can be cancelled if the server provide a cancellation routine using
 *         {@link #setCancellation(IgniteRunnable)} and the client calls {@link #cancel()} before the operation is
 *         complete.
 *     </li>
 * </ul>
 * The client and server sides of {@link TopicMessageFuture} communicate using Ignite topic-based messages.
 */
public class TopicMessageFuture<V> implements IgniteFuture<V> {
    /**
     * Unique topic name to to send operation results to.
     */
    private final String topic;

    /**
     * Cancellation routine or {@code null} if cancellation is not applicable.
     */
    private transient IgniteRunnable cancellation = null;

    /**
     * State.
     */
    private transient State state = State.ACTIVE;

    /**
     * Optional injected {@link Ignite}. If Ignite is not injected, one of the {@link Ignite} instances running in this
     * JVM will be used. Do not use this field to get {@link Ignite}: use {@link #getIgnite()} instead.
     */
    private transient Ignite injectedIgnite;

    /**
     * Constructor.
     */
    public TopicMessageFuture() {
        this.topic = UUID.randomUUID().toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get() throws IgniteException {
        try {
            return getQueue().take();
        }
        catch (InterruptedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(long timeout) throws IgniteException {
        return get(timeout, TimeUnit.MICROSECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(long timeout, TimeUnit unit) throws IgniteException {
        V res;

        try {
            res = getQueue().poll(timeout, unit);
        }
        catch (InterruptedException e) {
            throw new IgniteException(e);
        }

        if (res == null)
            throw new IgniteFutureTimeoutException("Operation did not complete in " + timeout + " " + unit);

        return res;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean cancel() throws IgniteException {
        if (state == State.ACTIVE) {


            state = State.CANCELLED;

            return true;
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCancelled() {
        return state == State.CANCELLED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDone() {
        return state != State.ACTIVE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void listen(IgniteInClosure<? super IgniteFuture<V>> lsnr) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void listenAsync(IgniteInClosure<? super IgniteFuture<V>> lsnr, Executor exec) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> IgniteFuture<T> chain(IgniteClosure<? super IgniteFuture<V>, T> doneCb) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> IgniteFuture<T> chainAsync(IgniteClosure<? super IgniteFuture<V>, T> doneCb, Executor exec) {
        return null;
    }

    /**
     * Set result of this {@link TopicMessageFuture}, which signals completion of the corresponding async operation. The
     * operation synchronously waits for the result to be delivered for the specified timeout in milliseconds.
     */
    public void resolve(V res, long timeout) throws InterruptedException, TimeoutException {
        Ignite ignite = getIgnite();
        IgniteMessaging igniteMsg = ignite.message(ignite.cluster().forRemotes());
        BlockingQueue<Object> q = new ArrayBlockingQueue<>(1);

        igniteMsg.localListen(topic, (nodeId, msg) -> {
            if (msg instanceof ResultReq)
                igniteMsg.send(topic, new Result(res));
            else if (msg instanceof ResultAck) {
                try {
                    q.put(msg);
                }
                catch (InterruptedException ignored) {
                }

                return false; // stop listening.
            }

            return true; // continue listening
        });

        igniteMsg.send(topic, new Result(res));

        try {
            if (q.poll(timeout, TimeUnit.MILLISECONDS) == null)
                throw new TimeoutException("Timeout occurred while waiting for the result delivery.");
        }
        finally {
            state = State.DONE;
        }
    }

    /**
     * Set cancellation routine or {@code null} if cancellation is not applicable.
     */
    public TopicMessageFuture<V> setCancellation(IgniteRunnable cancellation) {
        this.cancellation = cancellation;

        return this;
    }

    /**
     * Explicitly set {@link Ignite} node for this {@link TopicMessageFuture}.
     */
    public TopicMessageFuture<V> setIgnite(Ignite ignite) {
        injectedIgnite = ignite;

        return this;
    }

    /**
     * @return {@link BlockingQueue} listening for the operation result.
     */
    @SuppressWarnings("unchecked")
    private BlockingQueue<V> getQueue() {
        BlockingQueue<V> q = new ArrayBlockingQueue<>(1);
        Ignite ignite = getIgnite();
        IgniteMessaging igniteMsg = ignite.message(ignite.cluster().forRemotes());

        igniteMsg.localListen(topic, (nodeId, msg) -> {
            try {
                q.put((V)((Result)msg).result());

                igniteMsg.send(topic, new ResultAck());
            }
            catch (InterruptedException ignored) {
            }

            return false; // stop listening
        });

        igniteMsg.send(topic, new ResultReq(getIgnite().cluster().localNode().consistentId()));

        return q;
    }

    /**
     * @return {@link Ignite} node for this {@link TopicMessageFuture}.
     */
    private Ignite getIgnite() {
        return injectedIgnite == null ? IgniteHolder.INSTANCE : injectedIgnite;
    }

    /**
     * @return {@link IgniteMessaging} to remote nodes.
     */
    private Ignite getRemote() {
        return injectedIgnite == null ? IgniteHolder.INSTANCE : injectedIgnite;
    }

    /**
     * Thread-safe lazy Ignite initialization.
     */
    private static class IgniteHolder {
        /** Instance. */
        static final Ignite INSTANCE = Ignition.allGrids().get(0);
    }

    /**
     * {@link TopicMessageFuture} state.
     */
    private enum State {
        /**
         * The operation that this {@link TopicMessageFuture} is tracking is in progress.
         */
        ACTIVE,

        /**
         * The operation this {@link TopicMessageFuture} is tracking is complete: {@link
         * TopicMessageFuture#resolve(Object, long)} was called.
         */
        DONE,

        /**
         * The operation this {@link TopicMessageFuture} is cancelled: {@link TopicMessageFuture#cancel()} was called
         * before the operation was complete.
         */
        CANCELLED
    }

    /**
     * Client requests the operation result to be delivered from the server.
     */
    private static class ResultReq {
        /** Client id. */
        private final Object clientId;

        /** Constructor. */
        public ResultReq(Object clientId) {
            this.clientId = clientId;
        }
    }

    /**
     * Server sends result to the client.
     */
    private static class Result {
        /** Result. */
        private final Object res;

        /** Constructor. */
        public Result(Object res) {
            this.res = res;
        }

        /** @return Result. */
        public Object result() {
            return res;
        }
    }

    /**
     * Client confirms to the server the result was received.
     */
    private static class ResultAck {
    }

    /**
     * Client requests the server to cancel the operation.
     */
    private static class CancelReq {
    }
}
