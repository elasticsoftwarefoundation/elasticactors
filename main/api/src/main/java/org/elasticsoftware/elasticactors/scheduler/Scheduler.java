/*
 * Copyright 2013 - 2023 The Original Authors
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

package org.elasticsoftware.elasticactors.scheduler;

import org.elasticsoftware.elasticactors.ActorRef;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
@ParametersAreNonnullByDefault
public interface Scheduler {
    /**
     * Schedules a particular message to be send once
     *
     * @param message           the message to be send
     * @param receiver          the receiver of the message
     * @param delay             the delay before sending (a message is guaranteed to be send after this delay, but not exactly at this delay)
     * @param timeUnit          the {@link TimeUnit} to interpret the delay parameter
     * @return                  the serializable reference to the scheduled message that can be stored and used to manage it
     */
    @Nonnull
    ScheduledMessageRef scheduleOnce(
        Object message,
        ActorRef receiver,
        long delay,
        TimeUnit timeUnit);
}
