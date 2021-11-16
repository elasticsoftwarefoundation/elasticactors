package org.elasticsoftware.elasticactors.messaging.internal;

import javax.annotation.Nullable;

public final class InternalHashKeyUtils {

    private InternalHashKeyUtils() {
    }

    /**
     * Return the key of an object if it implements {@link MessageQueueBoundPayload}.
     */
    @Nullable
    public static String getMessageQueueAffinityKey(@Nullable Object object) {
        if (object instanceof MessageQueueBoundPayload) {
            String key = ((MessageQueueBoundPayload) object).getMessageQueueAffinityKey();
            if (key != null && !key.isEmpty()) {
                return key;
            }
        }
        return null;
    }

}
