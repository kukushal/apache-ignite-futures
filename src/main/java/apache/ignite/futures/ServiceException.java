package apache.ignite.futures;

/**
 * Service invocation failure.
 */
public class ServiceException extends RuntimeException {
    /** Serial version uid. */
    private static final long serialVersionUID = -1930515656936185076L;

    /**
     * Constructs a new exception with {@code null} as its detail message.
     */
    public ServiceException() {
    }

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message.
     */
    public ServiceException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified cause and a detail
     * message of <tt>(cause==null ? null : cause.toString())</tt>.
     *
     * @param cause the cause.
     */
    public ServiceException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message the detail message.
     * @param cause the cause.
     */
    public ServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the specified detail message,
     * cause, suppression enabled or disabled, and writable stack
     * trace enabled or disabled.
     *
     * @param message the detail message.
     * @param cause the cause.
     * @param enableSuppression whether or not suppression is enabled
     *                          or disabled
     * @param writableStackTrace whether or not the stack trace should
     *                           be writable
     */
    public ServiceException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}