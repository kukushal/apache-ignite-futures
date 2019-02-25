package apache.ignite.futures.topicmessage;

/** Cancellation confirmation sent from the server to the client. */
class CancelAck implements ServerResponse {
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
