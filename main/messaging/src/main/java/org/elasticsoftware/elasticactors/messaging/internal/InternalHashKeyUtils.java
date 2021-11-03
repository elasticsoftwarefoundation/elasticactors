package org.elasticsoftware.elasticactors.messaging.internal;

import javax.annotation.Nullable;

public class InternalHashKeyUtils {

    /**
     * Return the key of an object if it implements {@link MessageQueueBoundPayload}.
     */
    @Nullable
    public static String getMessageQueueAffinityKey(@Nullable Object object) {
        return object instanceof MessageQueueBoundPayload
            ? ((MessageQueueBoundPayload) object).getMessageQueueAffinityKey()
            : null;
    }

}
