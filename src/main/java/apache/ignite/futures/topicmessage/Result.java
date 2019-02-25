package apache.ignite.futures.topicmessage;

/** Result sent from the server to the client. */
class Result<V> implements ServerResponse {
    /** Result. */
    private final V value;

    /** Constructor. */
    Result(V value) {
        this.value = value;
    }

    /** @return Result or {@code null} if the operation has no result. */
    V value() {
        return value;
    }
}
