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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageSerializer;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import static org.elasticsoftware.elasticactors.messaging.SplittableUtils.calculateBucketForEmptyOrSingleActor;
import static org.elasticsoftware.elasticactors.messaging.SplittableUtils.groupByBucket;

/**
 * @author Joost van de Wijgerd
 */
public final class ImmutableInternalMessage extends AbstractTracedMessage
        implements InternalMessage, Serializable, Splittable<String, InternalMessage> {
    private final ActorRef sender;
    private final ImmutableList<ActorRef> receivers;
    private final UUID id;
    private final ByteBuffer payload;
    private final Object payloadObject;
    private final boolean durable;
    private final boolean undeliverable;
    private final int timeout;
    private transient byte[] serializedForm;

    public ImmutableInternalMessage(ActorRef sender, ActorRef receiver, ByteBuffer payload, Object payloadObject, boolean durable) {
        this(UUIDTools.createTimeBasedUUID(), sender, receiver, payload, payloadObject,durable,false);
    }

    public ImmutableInternalMessage(ActorRef sender, ImmutableList<ActorRef> receivers, ByteBuffer payload, Object payloadObject, boolean durable) {
        this(UUIDTools.createTimeBasedUUID(), sender, receivers, payload, payloadObject,durable,false, NO_TIMEOUT);
    }

    public ImmutableInternalMessage(ActorRef sender, ActorRef receiver, ByteBuffer payload, Object payloadObject, boolean durable, boolean undeliverable) {
        this(UUIDTools.createTimeBasedUUID(), sender, receiver, payload, payloadObject,durable,undeliverable);
    }

    public ImmutableInternalMessage(UUID id, ActorRef sender, ActorRef receiver, ByteBuffer payload, Object payloadObject, boolean durable, boolean undeliverable) {
        this(id, sender, ImmutableList.of(receiver), payload, payloadObject, durable, undeliverable, NO_TIMEOUT);
    }

    private ImmutableInternalMessage(UUID id,
            ActorRef sender,
            ImmutableList<ActorRef> receivers,
            ByteBuffer payload,
            Object payloadObject,
            boolean durable,
            boolean undeliverable,
            int timeout) {
        this.sender = sender;
        this.receivers = receivers;
        this.id = id;
        this.payload = payload;
        this.payloadObject = payloadObject;
        this.durable = durable;
        this.undeliverable = undeliverable;
        this.timeout = timeout;
    }

    public ImmutableInternalMessage(
            UUID id,
            ActorRef sender,
            ImmutableList<ActorRef> receivers,
            ByteBuffer payload,
            Object payloadObject,
            boolean durable,
            boolean undeliverable,
            int timeout,
            TraceContext traceContext,
            CreationContext creationContext) {
        super(traceContext, creationContext);
        this.sender = sender;
        this.receivers = receivers;
        this.id = id;
        this.payload = payload;
        this.payloadObject = payloadObject;
        this.durable = durable;
        this.undeliverable = undeliverable;
        this.timeout = timeout;
    }

    @Override
    @Nullable
    public ActorRef getSender() {
        return sender;
    }

    @Override
    public String getTypeAsString() {
        return payloadObject.getClass().getName();
    }

    @Nullable
    @Override
    public Class<?> getType() {
        return payloadObject.getClass();
    }

    @Override
    public ImmutableList<ActorRef> getReceivers() {
        return receivers;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public ByteBuffer getPayload() {
        return payload != null ? payload.asReadOnlyBuffer() : null;
    }

    @Override
    public <T> T getPayload(MessageDeserializer<T> deserializer) {
        return (T) payloadObject;
    }

    @Override
    public String getPayloadClass() {
        return payloadObject.getClass().getName();
    }

    @Override
    public boolean isDurable() {
        return durable;
    }

    @Override
    public boolean isUndeliverable() {
        return undeliverable;
    }

    @Override
    public int getTimeout() {
        return timeout;
    }

    @Override
    public boolean hasSerializedPayload() {
        return payload != null;
    }

    @Override
    public boolean hasPayloadObject() {
        return payloadObject != null;
    }

    @Override
    public byte[] toByteArray() {
        if(serializedForm == null) {
            serializedForm = InternalMessageSerializer.get().serialize(this);
        }
        return serializedForm;
    }

    @Override
    public InternalMessage copyOf() {
        return this;
    }

    @Override
    public ImmutableMap<Integer, InternalMessage> splitInBuckets(Hasher hasher, int buckets) {
        return receivers.size() <= 1
            ? ImmutableMap.of(calculateBucketForEmptyOrSingleActor(receivers, hasher, buckets), this)
            : groupByBucket(receivers, hasher, buckets, this::copyForReceivers);
    }

    private InternalMessage copyForReceivers(List<ActorRef> receivers) {
        return new ImmutableInternalMessage(
            UUIDTools.createTimeBasedUUID(),
            sender,
            ImmutableList.copyOf(receivers),
            payload,
            payloadObject,
            durable,
            undeliverable,
            timeout,
            getTraceContext(),
            getCreationContext()
        );
    }
}
