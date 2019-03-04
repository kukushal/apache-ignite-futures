package apache.ignite.futures.topicmessage;

/**
 * {@link TopicMessageFuture} state.
 */
enum State {
    /**
     * The instance of {@link TopicMessageFuture} is created on the server and not yet sent to the client. This state is
     * applicable to the server-side only, while the other states are applicable to both the client and server sides.
     */
    INIT,

    /**
     * The operation that this {@link TopicMessageFuture} is tracking is in progress.
     */
    ACTIVE,

    /**
     * The operation this {@link TopicMessageFuture} is tracking is complete: {@link TopicMessageFuture#resolve(Object,
     * long)} was called.
     */
    DONE,

    /**
     * The operation this {@link TopicMessageFuture} is tracking is cancelled: {@link TopicMessageFuture#cancel()}
     * was called before the operation was complete.
     */
    CANCELLED,

    /**
     * The operation this {@link TopicMessageFuture} is tracking is failed.
     */
    FAILED
}