package apache.ignite.futures;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;

import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * {@link IgniteFuture} implementation based on
 * <a href="https://apacheignite.readme.io/docs/messaging">Ignite Messaging</a>.
 * <p>
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
 * #setCancellation(IgniteRunnable, long)} and the client calls {@link #cancel()} before the operation is complete.
 * </li>
 * </ul>
 * The client and server sides of {@link TopicMessageFuture} communicate using Ignite topic-based messages.
 * <p>
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
 * {@link TopicMessageFuture} <b>client and server must be in separate Ignite nodes</b>.
 */
public class TopicMessageFuture<T> implements IgniteFuture<T>, Binarylizable {
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
    private T res;

    /**
     * Server-side cancellation routine or {@code null} if cancellation is not applicable.
     */
    private transient IgniteRunnable cancellation = null;

    /**
     * Max time in milliseconds for the client to wait for the {@link #cancellation} routine to complete. The timeout is
     * set by the server. Zero means cancellation is not applicable to this {@link TopicMessageFuture}.
     */
    private long cancelTimeout = 0;

    /**
     * {@link Ignite} instance used by this {@link TopicMessageFuture}.
     */
    private transient Ignite ignite = Ignition.allGrids().get(0);

    /**
     * Client-side queue of the server responses.
     */
    private transient BlockingQueue<ServerResponse> srvRspQueue;

    /**
     * Server-side {@link CancelReq} listener or {@code null} if cancellation is not applicable.
     */
    private transient IgniteBiPredicate<UUID, Object> cancelReqLsnr;

    /**
     * Client-side listeners list.
     */
    private transient Collection<IgniteInClosure<? super TopicMessageFuture<T>>> lsnrs;

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        // The server is sending the future to the client.
        if (state == State.INIT)
            state = State.ACTIVE;

        writer.writeString("topic", topic);
        writer.writeEnum("state", state);
        writer.writeObject("res", res);
        writer.writeLong("cancelTimeout", cancelTimeout);

        // See createServerResponseQueue() method documentation for the cancellation details.
        if (cancellation != null && cancelReqLsnr == null) {
            IgniteMessaging igniteMsg = ignite.message();

            cancelReqLsnr = (nodeId, msg) -> {
                if (msg instanceof CancelReq) {
                    String failure = null;

                    try {
                        cancellation.run();

                        state = State.CANCELLED;
                    }
                    catch (Exception ex) {
                        failure = ex.getMessage();
                    }

                    igniteMsg.send(topic, new CancelAck(failure));

                    return false; // stop listening
                }

                return true; // continue listening
            };

            igniteMsg.localListen(topic, cancelReqLsnr);
        }
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
        cancelTimeout = reader.readLong("cancelTimeout");

        srvRspQueue = createServerResponseQueue();
        lsnrs = new ConcurrentLinkedQueue<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T get() throws IgniteException {
        if (srvRspQueue == null)
            throw new IllegalStateException("Trying to call client-side method on the server side");

        ServerResponse msg;

        try {
            msg = srvRspQueue.take();
        }
        catch (InterruptedException e) {
            throw new IgniteException(e);
        }

        return unwrapResult(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T get(long timeout) throws IgniteException {
        return get(timeout, TimeUnit.MICROSECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T get(long timeout, TimeUnit unit) throws IgniteException {
        if (srvRspQueue == null)
            throw new IllegalStateException("Trying to call client-side method on the server side");

        ServerResponse msg;

        try {
            msg = srvRspQueue.poll(timeout, unit);
        }
        catch (InterruptedException e) {
            throw new IgniteException(e);
        }

        if (msg == null)
            throw new IgniteFutureTimeoutException("Operation did not complete in " + timeout + " " + unit);

        return unwrapResult(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean cancel() throws IgniteException {
        if (srvRspQueue == null)
            throw new IllegalStateException("Trying to call client-side method on the server side");

        // See createServerResponseQueue() method documentation for the cancellation details.
        if (cancelTimeout > 0 && state == State.ACTIVE) {
            IgniteMessaging igniteMsg = ignite.message();

            igniteMsg.send(topic, new CancelReq());

            ServerResponse msg;

            try {
                msg = srvRspQueue.poll(cancelTimeout, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }

            if (msg == null)
                throw new IgniteFutureTimeoutException(
                    "Cancellation did not complete in " + cancelTimeout + " milliseconds"
                );

            if (msg instanceof CancelAck) {
                String failure = ((CancelAck)msg).failure();

                if (failure != null)
                    throw new IgniteException(failure);

                return true;
            }
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
    public void listen(IgniteInClosure<? super IgniteFuture<T>> lsnr) {
        Objects.requireNonNull(lsnr);

        if (lsnrs == null)
            throw new IllegalStateException("Trying to call client-side method on the server side");

        if (state != State.ACTIVE)
            lsnr.apply(this);
        else
            lsnrs.add(lsnr);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void listenAsync(IgniteInClosure<? super IgniteFuture<T>> lsnr, Executor exec) {
        Objects.requireNonNull(lsnr);
        Objects.requireNonNull(exec);

        listen(new AsyncListener(lsnr, exec));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <U> IgniteFuture<U> chain(IgniteClosure<? super IgniteFuture<T>, U> doneCb) {
        Objects.requireNonNull(doneCb);

        return new ChainedFuture<>(this, doneCb, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <U> IgniteFuture<U> chainAsync(IgniteClosure<? super IgniteFuture<T>, U> doneCb, Executor exec) {
        Objects.requireNonNull(doneCb);
        Objects.requireNonNull(exec);

        return new ChainedFuture<>(this, doneCb, exec);
    }

    /**
     * SERVER-SIDE API: set result of the async operation.
     *
     * @param res Async operation result.
     * @return This {@link TopicMessageFuture} in a {@link State#DONE} state.
     */
    public TopicMessageFuture<T> resolve(T res) {
        // See createServerResponseQueue() method documentation for the implementation details.
        if (state != State.CANCELLED) {
            if (state == State.INIT)
                this.res = res;
            else {
                IgniteMessaging igniteMsg = ignite.message();

                igniteMsg.send(topic, new Result<>(res));

                if (cancelReqLsnr != null) {
                    igniteMsg.stopLocalListen(topic, cancelReqLsnr);

                    cancelReqLsnr = null;
                }
            }

            state = State.DONE;
        }

        return this;
    }

    /**
     * SERVER-SIDE API: Set cancellation routine or {@code null} if cancellation is not applicable.
     *
     * @param cancellation Cancellation routine.
     * @param cancelTimeout Cancellation timeout.
     * @return This {@link TopicMessageFuture}.
     */
    public TopicMessageFuture<T> setCancellation(IgniteRunnable cancellation, long cancelTimeout) {
        if (cancellation != null && cancelTimeout <= 0)
            throw new IllegalArgumentException("cancelTimeout must be a positive number");

        this.cancellation = cancellation;
        this.cancelTimeout = cancelTimeout;

        return this;
    }

    /**
     * Explicitly set {@link Ignite} node for this {@link TopicMessageFuture}. The first Ignite node started on the
     * server is used if Ignite is not explicitly set. This method is needed only in multiple Ignite nodes per JVM
     * environments (which are normally developer test environments).
     *
     * @param ignite Ignite node.
     * @return This {@link TopicMessageFuture}.
     */
    public TopicMessageFuture<T> setIgnite(Ignite ignite) {
        Objects.requireNonNull(ignite);

        this.ignite = ignite;

        return this;
    }

    /**
     * Setup a client-side queue listening for the server responses.
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
     * Optionally, the server might decide to execute the operation synchronously and use {@link #resolve(T)} to set the
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
     * <li>
     * The client can try cancelling the operation using {@link #cancel()}. Cancellation is possible only if the sever
     * supports cancellation ({@link #cancelTimeout} > 0) and the future is active. Cancellation is synchronous: the
     * client sends {@link CancelReq} to the server and waits for the {@link CancelAck} for the {@link #cancelTimeout}
     * milliseconds max, which is specified on the server side.
     * </li>
     * </ol>
     *
     * @return An instance of {@link BlockingQueue} listening for the server responses.
     */
    private BlockingQueue<ServerResponse> createServerResponseQueue() {
        // Only one server response can be received: either result of cancellation confirmation.
        BlockingQueue<ServerResponse> q = new ArrayBlockingQueue<>(1);

        if (state == State.DONE) {
            // The server executed and completed operation synchronously.
            try {
                q.put(new Result<>(res));
            }
            catch (InterruptedException ignored) {
            }
        }
        else {
            IgniteMessaging igniteMsg = ignite.message();

            igniteMsg.localListen(topic, (nodeId, msg) -> {
                if (msg instanceof ServerResponse) {
                    if (msg instanceof Result)
                        state = State.DONE;
                    else if (msg instanceof CancelAck && ((CancelAck)msg).failure() == null)
                        state = State.CANCELLED;

                    try {
                        q.put((ServerResponse)msg);
                    }
                    catch (InterruptedException ignored) {
                    }

                    lsnrs.forEach(l -> {
                        try {
                            l.apply(this);
                        }
                        catch (Exception ex) {
                            ignite.log().error("Failed to notify listener", ex);
                        }
                    });

                    lsnrs.clear();

                    return false; // stop listening
                }

                return true; // continue listening
            });
        }

        return q;
    }

    /** @return Result from the message received from the server. */
    @SuppressWarnings("unchecked")
    private T unwrapResult(ServerResponse msg) {
        if (msg instanceof Result)
            return ((Result<T>)msg).result();
        else if (msg instanceof CancelAck)
            throw new IgniteFutureCancelledException(((CancelAck)msg).failure());
        else
            throw new IgniteException("Unsupported message received from the server: " + msg);
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

    /** Server response. */
    private interface ServerResponse {
    }

    /** Result sent from the server to the client. */
    private static class Result<V> implements ServerResponse {
        /** Result. */
        private final V res;

        /** Constructor. */
        Result(V res) {
            this.res = res;
        }

        /** @return Result or {@code null} if the operation has no result. */
        V result() {
            return res;
        }
    }

    /** Cancellation request sent from the client to the server. */
    private static class CancelReq {
    }

    /** Cancellation confirmation sent from the server to the client. */
    private static class CancelAck implements ServerResponse {
        /** Failure message. */
        private final String failure;

        /** Constructor. */
        CancelAck(String failure) {
            this.failure = failure;
        }

        /** @return Cancellation failure or {@code null} is cancellation succeeded. */
        String failure() {
            return failure;
        }
    }

    /**
     * {@link TopicMessageFuture} client-side listener to call with the specified {@link Executor}.
     */
    private final class AsyncListener implements IgniteInClosure<IgniteFuture<T>> {
        /** Listener. */
        private final IgniteInClosure<? super IgniteFuture<T>> lsnr;

        /** Executor. */
        private final Executor exec;

        /** Constructor. */
        AsyncListener(IgniteInClosure<? super IgniteFuture<T>> lsnr, Executor exec) {
            this.lsnr = lsnr;
            this.exec = exec;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteFuture<T> fut) {
            exec.execute(() -> lsnr.apply(fut));
        }
    }

    /**
     * Simple non-distributed {@link IgniteFuture} implementation returned to the client as a result of {@link
     * TopicMessageFuture} chaining.
     */
    private static class ChainedFuture<U, V> implements IgniteFuture<U> {
        /** Target. */
        private final IgniteFuture<V> target;

        /** Implementation. */
        private final CompletableFuture<U> impl = new CompletableFuture<>();

        /** Constructor. */
        ChainedFuture(IgniteFuture<V> target, IgniteClosure<? super IgniteFuture<V>, U> doneCb, Executor exec) {
            this.target = target;

            if (exec == null)
                target.listen(fut -> impl.complete(doneCb.apply(fut)));
            else
                target.listenAsync(fut -> impl.complete(doneCb.apply(fut)), exec);
        }

        /** {@inheritDoc} */
        @Override public U get() throws IgniteException {
            try {
                return impl.get();
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public U get(long timeout) throws IgniteException {
            return get(timeout, TimeUnit.MICROSECONDS);
        }

        /** {@inheritDoc} */
        @Override public U get(long timeout, TimeUnit unit) throws IgniteException {
            try {
                return impl.get(timeout, unit);
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws IgniteException {
            boolean res = target.cancel();

            impl.cancel(false);

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean isCancelled() {
            return impl.isCancelled();
        }

        /** {@inheritDoc} */
        @Override public boolean isDone() {
            return impl.isDone();
        }

        /** {@inheritDoc} */
        @Override public void listen(IgniteInClosure<? super IgniteFuture<U>> lsnr) {
            impl.thenAccept(ignored -> lsnr.apply(this));
        }

        /** {@inheritDoc} */
        @Override public void listenAsync(IgniteInClosure<? super IgniteFuture<U>> lsnr, Executor exec) {
            impl.thenAcceptAsync(ignored -> lsnr.apply(this), exec);
        }

        /** {@inheritDoc} */
        @Override public <W> IgniteFuture<W> chain(IgniteClosure<? super IgniteFuture<U>, W> doneCb) {
            return new ChainedFuture<>(this, doneCb, null);
        }

        /** {@inheritDoc} */
        @Override
        public <W> IgniteFuture<W> chainAsync(IgniteClosure<? super IgniteFuture<U>, W> doneCb, Executor exec) {
            return new ChainedFuture<>(this, doneCb, exec);
        }
    }
}
