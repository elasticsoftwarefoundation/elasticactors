package org.elasticsoftware.elasticactors.messaging.internal;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public class CancelScheduledMessageMessage implements Serializable {
    private final UUID messageId;
    private final long fireTime;

    public CancelScheduledMessageMessage(UUID messageId, long fireTime) {
        this.messageId = messageId;
        this.fireTime = fireTime;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public long getFireTime() {
        return fireTime;
    }
}
