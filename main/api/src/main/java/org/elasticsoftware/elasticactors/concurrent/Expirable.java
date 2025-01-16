/*
 * Copyright 2013 - 2025 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

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
