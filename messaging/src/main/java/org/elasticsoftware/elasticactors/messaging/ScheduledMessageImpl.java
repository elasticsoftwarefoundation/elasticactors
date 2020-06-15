/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.messaging;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageKey;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public final class ScheduledMessageImpl extends AbstractTracedMessage implements ScheduledMessage {
    private final UUID id;
    private final long fireTime; // milliseconds since epoch
    private final ActorRef sender;
    private final ActorRef receiver;
    private final Class messageClass;
    private final byte[] messageBytes;
    private final ScheduledMessageKey key;

    public ScheduledMessageImpl(long fireTime, ActorRef sender, ActorRef receiver, Class messageClass,byte[] messageBytes) {
        this(UUIDTools.createTimeBasedUUID(),fireTime,sender,receiver, messageClass, messageBytes);
    }

    private ScheduledMessageImpl(
            UUID id,
            long fireTime,
            ActorRef sender,
            ActorRef receiver,
            Class messageClass,
            byte[] messageBytes) {
        this.id = id;
        this.fireTime = fireTime;
        this.sender = sender;
        this.receiver = receiver;
        this.messageClass = messageClass;
        this.messageBytes = messageBytes;
        this.key = new ScheduledMessageKey(id, fireTime);
    }

    public ScheduledMessageImpl(
            UUID id,
            long fireTime,
            ActorRef sender,
            ActorRef receiver,
            Class messageClass,
            byte[] messageBytes,
            TraceContext traceContext,
            CreationContext creationContext) {
        super(traceContext, creationContext);
        this.id = id;
        this.fireTime = fireTime;
        this.sender = sender;
        this.receiver = receiver;
        this.messageClass = messageClass;
        this.messageBytes = messageBytes;
        this.key = new ScheduledMessageKey(id, fireTime);
    }

    /**
     * Constructor that is used to remove the ScheduledMessage. The id and fireTime fields make up the unique key
     *
     * @param id
     * @param fireTime
     */
    public ScheduledMessageImpl(UUID id,long fireTime) {
        this(id,fireTime,null,null,null,null);
    }

    @Override
    public ScheduledMessageKey getKey() {
        return key;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public ActorRef getReceiver() {
        return receiver;
    }

    @Override
    public Class getMessageClass() {
        return messageClass;
    }

    @Override
    public byte[] getMessageBytes() {
        return messageBytes;
    }

    public <T> T getPayload(MessageDeserializer<T> deserializer) throws IOException {
        return deserializer.deserialize(ByteBuffer.wrap(messageBytes));
    }

    @Override
    @Nullable
    public ActorRef getSender() {
        return sender;
    }

    @Override
    public String getType() {
        return messageClass.getName();
    }

    @Override
    public long getFireTime(TimeUnit timeUnit) {
        return timeUnit.convert(fireTime,TimeUnit.MILLISECONDS);
    }

    private long now() {
        return System.currentTimeMillis();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(fireTime - now(),TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        if (other == this)
            return 0;
        long d = (getDelay(TimeUnit.MILLISECONDS) -
                other.getDelay(TimeUnit.MILLISECONDS));
        if(d != 0) {
            return (d < 0) ? -1 : 1;
        } else {
            // use the ordering of the id as well in case the other Delayed is a ScheduledMessage as well
            if(other instanceof ScheduledMessage) {
                return getId().compareTo(((ScheduledMessage)other).getId());
            } else {
                return 0;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ScheduledMessage)) {
            return false;
        }

        ScheduledMessage that = (ScheduledMessage) o;

        return key.equals(that.getKey());
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
