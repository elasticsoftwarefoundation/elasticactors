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

package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.tracing.TracedMessage;

import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public interface ScheduledMessage extends Delayed, TracedMessage {

    ScheduledMessageKey getKey();

    /**
     * The UUID (time based) for this message
     *
     * @return
     */
    UUID getId();

    /**
     * The receiver of the scheduled message
     *
     * @return
     */
    ActorRef getReceiver();

    /**
     * The message
     *
     * @return
     */
    ByteBuffer getMessageBytes();

    /**
     * The sender
     *
     * @return
     */
    @Override
    @Nullable
    ActorRef getSender();

    /**
     * The absolute time on which this message should be send
     *
     * @param timeUnit
     * @return
     */
    long getFireTime(TimeUnit timeUnit);

    Class getMessageClass();

    @Nullable
    String getMessageQueueAffinityKey();

    ScheduledMessage copyForRescheduling(long newFireTime);
}
