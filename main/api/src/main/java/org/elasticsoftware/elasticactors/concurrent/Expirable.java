package org.elasticsoftware.elasticactors.concurrent;

import java.util.concurrent.TimeUnit;

public interface Expirable {

    long TEMP_ACTOR_TIMEOUT_MIN = clamp(
        Long.parseLong(System.getProperty(
            "ea.tempActor.timeout.min",
            Long.toString(TimeUnit.SECONDS.toMillis(1))
        )),
        1,
        Long.MAX_VALUE
    );
    long TEMP_ACTOR_TIMEOUT_MAX = clamp(
        Long.parseLong(System.getProperty(
            "ea.tempActor.timeout.max",
            Long.toString(TimeUnit.DAYS.toMillis(2))
        )),
        TEMP_ACTOR_TIMEOUT_MIN,
        Long.MAX_VALUE
    );
    long TEMP_ACTOR_TIMEOUT_DEFAULT = clamp(
        Long.parseLong(System.getProperty(
            "ea.tempActor.timeout.default",
            Long.toString(TimeUnit.DAYS.toMillis(1))
        )),
        TEMP_ACTOR_TIMEOUT_MIN,
        TEMP_ACTOR_TIMEOUT_MAX
    );

    static long clamp(long value, long minValue, long maxValue) {
        return Math.min(Math.max(minValue, value), maxValue);
    }

    /**
     * The timeout instant for this Expirable object.
     */
    long getExpirationTime();
}