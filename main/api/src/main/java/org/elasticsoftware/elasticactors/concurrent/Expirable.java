package org.elasticsoftware.elasticactors.concurrent;

import java.util.concurrent.TimeUnit;

public interface Expirable {

    long TEMP_ACTOR_TIMEOUT_MIN = TimeUnit.SECONDS.toMillis(1);
    long TEMP_ACTOR_TIMEOUT_MAX = TimeUnit.DAYS.toMillis(1);

    static long clamp(long value, long minValue, long maxValue) {
        return Math.min(Math.max(minValue, value), maxValue);
    }

    /**
     * The timeout instant for this Expirable object.
     */
    long getExpirationTime();
}
