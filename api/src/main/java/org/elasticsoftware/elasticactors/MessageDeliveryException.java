package org.elasticsoftware.elasticactors;

/**
 * @author Joost van de Wijgerd
 */
public final class MessageDeliveryException extends RuntimeException {
    private final boolean recoverable;

    public MessageDeliveryException(String message,boolean recoverable) {
        super(message);
        this.recoverable = recoverable;
    }

    public MessageDeliveryException(String message, Throwable cause, boolean recoverable) {
        super(message, cause);
        this.recoverable = recoverable;
    }

    public boolean isRecoverable() {
        return recoverable;
    }
}
