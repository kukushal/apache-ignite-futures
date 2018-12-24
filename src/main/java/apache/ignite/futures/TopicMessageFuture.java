package apache.ignite.futures;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
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
 * <p><p>
 * The future consists of separate client and server side implementations with separate APIs:
 * <ul>
 * <li>The server creates the future and returns it to the client.</li>
 * <li>
 * The server starts asynchronous operation and calls {@link #resolve(Object)} when the operation is complete.
 * </li>
 * <li>
 * The client calls {@link #get()} to synchronously wait for the operation result or {@link #chain}/{@link
 * #listen(IgniteInClosure)} to asynchronously receive result of the operation.
 * </li>
 * <li>
 * The asynchronous operation can be cancelled if the server provides a cancellation routine using {@link
 * #setCancellation(IgniteRunnable)} and the client calls {@link #cancel()} before the operation is complete.
 * </li>
 * </ul>
 * The client and server sides of {@link TopicMessageFuture} communicate using Ignite topic-based messages.
 * <p><p>
 * Async operations returning instances of {@link TopicMessageFuture} are <b>NOT fault-tolerant</b>:
 * <ul>
 * <li>
 * If Ignite server that initiated an async operation fails after returning {@link TopicMessageFuture} to the client,
 * the client will never get the operation's result.
 * </li>
 * <li>
 * Ignite client that called an async operation and then failed cannot resume listening for the operation result.
 * </li>
 * </ul>
 */
public class TopicMessageFuture<V> implements IgniteFuture<V>, Binarylizable {
    /**
     * Unique topic name generated on the server to use for the client-server communication.
     */
    private String topic = UUID.randomUUID().toString();

    /**
     * State: initially the {@link TopicMessageFuture} is created on the server in the {@link State#INIT} state.
     */
    private State state = State.INIT;

    /**
     * The async operation's result if applicable.
     */
    private V res;

    /**
     * The flag indicates if the server supports cancelling the async operation.
     */
    private boolean isCancellable = false;

    /**
     * Server-side cancellation routine or {@code null} if cancellation is not applicable.
     */
    private transient IgniteRunnable cancellation = null;

    /**
     * Optional injected {@link Ignite}. If Ignite is not injected, one of the {@link Ignite} instances running in this
     * JVM will be used. Do not use this field to get {@link Ignite}: use {@link #getIgnite()} instead.
     */
    private transient Ignite injectedIgnite;

    /**
     * Client-side result holder.
     */
    private transient BlockingQueue<V> clientResultQueue;

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        // The server is sending the future to the client
        if (state == State.INIT)
            state = State.ACTIVE;

        writer.writeString("topic", topic);
        writer.writeEnum("state", state);
        writer.writeObject("res", res);
        writer.writeBoolean("isCancellable", isCancellable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readBinary(BinaryReader reader) throws BinaryObjectException {
        // The client is receiving the future from the server
        topic = reader.readString("topic");
        state = reader.readEnum("state");
        res = reader.readObject("res");
        isCancellable = reader.readBoolean("isCancellable");

        clientResultQueue = createClientResultQueue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get() throws IgniteException {
        if (clientResultQueue == null)
            throw new IllegalStateException("Trying to call client-side method on the server side");

        try {
            return clientResultQueue.take();
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
        if (clientResultQueue == null)
            throw new IllegalStateException("Trying to call client-side method on the server side");

        V res;

        try {
            res = clientResultQueue.poll(timeout, unit);
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
     * SERVER-SIDE API: set result of the async operation.
     */
    public TopicMessageFuture<V> resolve(V res) throws InterruptedException, TimeoutException {
        // See createClientResultQueue() method documentation for the implementation details.
        if (state == State.INIT)
            this.res = res;
        else {
            Ignite ignite = getIgnite();
            IgniteMessaging igniteMsg = ignite.message();

            igniteMsg.send(topic, new Result<>(res));
        }

        state = State.DONE;

        return this;
    }

    /**
     * SERVER-SIDE API: Set cancellation routine or {@code null} if cancellation is not applicable.
     */
    public TopicMessageFuture<V> setCancellation(IgniteRunnable cancellation) {
        this.cancellation = cancellation;

        return this;
    }

    /**
     * Explicitly set {@link Ignite} node for this {@link TopicMessageFuture}. The first Ignite node started on the
     * server is used ig Ignite is not explicitly set.
     */
    public TopicMessageFuture<V> setIgnite(Ignite ignite) {
        injectedIgnite = ignite;

        return this;
    }

    /**
     * CLIENT-SIDE API {@link #get()} implementation.
     * <p>
     * <p>
     * <b>DESIGN NOTES</b>
     * <ol>
     * <li>
     * The client calls an async operation on the server. The Server initiates the async operation, immediately creating
     * an instance of {@link TopicMessageFuture} and returning it to the client, which also includes:
     * <ul>
     * <li>A random serializable {@link #topic} name is generated on the server and transmitted to the client.</li>
     * <li>The server starts listening to the {@link #topic}.</li>
     * <li>
     * Optionally, the server might decide to execute the operation synchronously and use {@link #resolve(V)} to set the
     * operation result before returning the instance of {@link TopicMessageFuture} to the client. In this case the
     * server does not create the {@link #topic}.
     * </li>
     * </ul>
     * </li>
     * <li>
     * The client receives the instance of {@link TopicMessageFuture} and starts listening to the received {@link
     * #topic} unless the instance already contains the operation result.
     * </li>
     * <li>
     * When the async operation is complete, the server calls {@link #resolve(Object)} to send the operation's result to
     * the client or {@code null} if the operation has no result to return.
     * </li>
     * <li>The client receives the result and stores it in {@link #res}.</li>
     * <li>
     * Use one of these client-side APIs to retrieve the result once it becomes available: {@link #get()}, {@link
     * #get(long)}, {@link #get(long, TimeUnit)}, {@link #listen(IgniteInClosure)}, {@link #listenAsync(IgniteInClosure,
     * Executor)}, {@link #chain(IgniteClosure)}, {@link #chainAsync(IgniteClosure, Executor)}.
     * </li>
     * </ol>
     *
     * @return An instance of {@link BlockingQueue} listening for the operation result.
     */
    @SuppressWarnings("unchecked")
    private BlockingQueue<V> createClientResultQueue() {
        BlockingQueue<V> q = new ArrayBlockingQueue<>(1);

        if (state == State.DONE) {
            // The server executed and completed operation synchronously.
            try {
                q.put(res);
            }
            catch (InterruptedException ignored) {
            }
        }
        else {
            Ignite ignite = getIgnite();
            IgniteMessaging igniteMsg = ignite.message();

            igniteMsg.localListen(topic, (nodeId, msg) -> {
                try {
                    q.put((V)((Result)msg).result());

                    state = State.DONE;
                }
                catch (InterruptedException ignored) {
                }

                return false; // stop listening
            });
        }

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
     * Thread-safe lazy default Ignite instance initialization. The default Ignite instance is used unless the user
     * explicitly specified Ignite using {@link #setIgnite(Ignite)}.
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
         * The instance of {@link TopicMessageFuture} is created on the server and not yet sent to the client. This
         * state is applicable to the server-side only, while the other states are applicable to both the client and
         * server sides.
         */
        INIT,

        /**
         * The operation that this {@link TopicMessageFuture} is tracking is in progress.
         */
        ACTIVE,

        /**
         * The operation this {@link TopicMessageFuture} is tracking is complete: {@link
         * TopicMessageFuture#resolve(Object)} was called.
         */
        DONE,

        /**
         * The operation this {@link TopicMessageFuture} is cancelled: {@link TopicMessageFuture#cancel()} was called
         * before the operation was complete.
         */
        CANCELLED
    }

    /**
     * Server sends result to the client.
     */
    private static class Result<V> {
        /** Result. */
        private final V res;

        /** Constructor. */
        Result(V res) {
            this.res = res;
        }

        /** @return Result. */
        V result() {
            return res;
        }
    }
}
