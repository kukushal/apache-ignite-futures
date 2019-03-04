package apache.ignite.futures.topicmessage;

/** Result sent from the server to the client. */
class Result<V> {
    /** Result. */
    private final V value;

    /** Failure. */
    private final String failure;

    /** Constructor. */
    Result(V value, String failure) {
        this.value = value;
        this.failure = failure;
    }

    /** @return Result or {@code null} if the operation has no result or the operation failed. */
    V value() {
        return value;
    }

    /** @return Failure or {@code null} if the operation was successful. */
    String failure() {
        return failure;
    }
}
