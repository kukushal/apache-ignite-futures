package apache.ignite.futures.topicmessage;

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
import java.util.concurrent.CountDownLatch;
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
 * The server starts asynchronous operation and calls {@link #resolve(T, long)} when the operation is complete.
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
    /** Unique topic name generated on the server to use for the client-server communication. */
    private String topic = UUID.randomUUID().toString();

    /**
     * State: initially the {@link TopicMessageFuture} is created on the server in the {@link State#INIT} state.
     */
    private State state = State.INIT;

    /** The async operation's result if applicable. */
    private T result;

    /** Server-side cancellation routine or {@code null} if cancellation is not applicable. */
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

    /** Client-side queue of the server responses. */
    private transient BlockingQueue<Object> srvRspQueue;

    /** Client-side listeners list. */
    private transient Collection<IgniteInClosure<? super TopicMessageFuture<T>>> lsnrs;

    /** Server-side client requests listener. */
    private transient IgniteBiPredicate<UUID, Object> clientReqLsnr;

    /** Server-side "is client ready to receive result?" latch. */
    private final transient CountDownLatch clientReadyLatch = new CountDownLatch(1);

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
        writer.writeObject("result", result);
        writer.writeLong("cancelTimeout", cancelTimeout);

        // See createServerResponseQueue() method for the protocol details.
        if (state != State.DONE) {
            IgniteMessaging igniteMsg = ignite.message();

            clientReqLsnr = (nodeId, msg) -> !clientSideHandler(msg);

            igniteMsg.localListen(topic, clientReqLsnr);
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
        result = reader.readObject("result");
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

        Object msg;

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

        Object msg;

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

            Object msg;

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
     * @param result Async operation result.
     * @param resolveTimeout Max time in milliseconds to wait for the client to become ready to receive the result.
     * @return This {@link TopicMessageFuture} in a {@link State#DONE} state.
     */
    public TopicMessageFuture<T> resolve(T result, long resolveTimeout) throws InterruptedException {
        // See createServerResponseQueue() method documentation for the implementation details.
        if (state != State.CANCELLED) {
            if (state == State.INIT)
                this.result = result;
            else {
                IgniteMessaging igniteMsg = ignite.message();

                clientReadyLatch.await(resolveTimeout, TimeUnit.MILLISECONDS);

                igniteMsg.send(topic, new Result<>(result));

                if (clientReqLsnr != null) {
                    igniteMsg.stopLocalListen(topic, clientReqLsnr);

                    clientReqLsnr = null;
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
     * Optionally, the server might decide to execute the operation synchronously and use {@link #resolve(T, long)} to
     * set the operation result before returning the instance of {@link TopicMessageFuture} to the client. In this case
     * the server does not create the {@link #topic}.
     * </li>
     * </ul>
     * </li>
     * <li>
     * The client receives the instance of {@link TopicMessageFuture}, sends {@link ResultReq} to tbe server to indicate
     * it is ready to receive the operaiton result and starts listening to the received {@link #topic}.
     * </li>
     * <li>
     * When the async operation is complete, the server makes sure the {@link ResultReq} has been received and calls
     * {@link #resolve(T, long)} to send the operation's result to the client or {@code null} if the operation has no
     * result to return.
     * </li>
     * <li>The client receives the result and stores it in {@link #result}.</li>
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
    private BlockingQueue<Object> createServerResponseQueue() {
        // Only one server response can be received: either result of cancellation confirmation.
        BlockingQueue<Object> q = new ArrayBlockingQueue<>(1);

        if (state == State.DONE) {
            // The server executed and completed operation synchronously.
            try {
                q.put(new Result<>(result));
            }
            catch (InterruptedException ignored) {
            }
        }
        else {
            IgniteMessaging igniteMsg = ignite.message();

            igniteMsg.localListen(topic, (nodeId, msg) -> {
                boolean processed = serverSideHandler(msg);

                if (processed) {
                    try {
                        q.put(msg);
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
                }

                return !processed;
            });

            igniteMsg.send(topic, new ResultReq());
        }

        return q;
    }

    /**
     * Server-side message loop.
     * @param msg Message.
     * @return {@code true} if the message was processed.
     */
    private boolean serverSideHandler(Object msg) {
        boolean processed = true;

        if (msg instanceof Result)
            state = State.DONE;
        else if (msg instanceof CancelAck) {
            if (((CancelAck)msg).failure() == null)
                state = State.CANCELLED;
        }
        else
            processed = false;

        return processed;
    }

    /**
     * Client-side message loop.
     * @param msg A message from the server.
     * @return {@code true} if the message was processed.
     */
    private boolean clientSideHandler(Object msg) {
        boolean processed = true;

        if (msg instanceof CancelReq) {
            String failure = null;

            try {
                cancellation.run();

                state = State.CANCELLED;
            }
            catch (Exception ex) {
                failure = ex.getMessage();
            }

            ignite.message().send(topic, new CancelAck(failure));
        }
        else if (msg instanceof ResultReq)
            clientReadyLatch.countDown();
        else
            processed = false;

        return processed;
    }

    /** @return Result from the message received from the server. */
    @SuppressWarnings("unchecked")
    private T unwrapResult(Object msg) {
        if (msg instanceof Result)
            return ((Result<T>)msg).value();
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
