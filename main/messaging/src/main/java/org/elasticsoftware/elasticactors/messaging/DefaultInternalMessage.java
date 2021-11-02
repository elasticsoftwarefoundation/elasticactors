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
import org.elasticsoftware.elasticactors.serialization.SerializationContext;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageSerializer;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.elasticsoftware.elasticactors.messaging.SplittableUtils.calculateHash;
import static org.elasticsoftware.elasticactors.messaging.SplittableUtils.groupByHashValue;

/**
 * @author Joost van de Wijgerd
 */
public final class DefaultInternalMessage extends AbstractTracedMessage
        implements InternalMessage, Serializable, Splittable<String, InternalMessage> {
    private final ActorRef sender;
    private final ImmutableList<ActorRef> receivers;
    private final UUID id;
    private final ByteBuffer payload;
    private final String payloadClass;
    private final boolean durable;
    private final boolean undeliverable;
    private final int timeout;
    private transient byte[] serializedForm;

    public DefaultInternalMessage(ActorRef sender, ActorRef receiver, ByteBuffer payload, String payloadClass,boolean durable) {
        this(UUIDTools.createTimeBasedUUID(), sender, receiver, payload, payloadClass, durable, false);
    }

    public DefaultInternalMessage(ActorRef sender, ImmutableList<ActorRef> receivers, ByteBuffer payload, String payloadClass,boolean durable) {
        this(UUIDTools.createTimeBasedUUID(), sender, receivers, payload, payloadClass, durable, false, NO_TIMEOUT);
    }

    public DefaultInternalMessage(ActorRef sender, ImmutableList<ActorRef> receivers, ByteBuffer payload, String payloadClass,boolean durable, int timeout) {
        this(UUIDTools.createTimeBasedUUID(), sender, receivers, payload, payloadClass, durable, false, timeout);
    }

    public DefaultInternalMessage(ActorRef sender, ActorRef receiver,ByteBuffer payload, String payloadClass, boolean durable, boolean undeliverable) {
        this(UUIDTools.createTimeBasedUUID(), sender, receiver, payload, payloadClass, durable, undeliverable);
    }

    public DefaultInternalMessage(ActorRef sender, ActorRef receiver,ByteBuffer payload, String payloadClass, boolean durable, boolean undeliverable, int timeout) {
        this(UUIDTools.createTimeBasedUUID(), sender, ImmutableList.of(receiver), payload, payloadClass, durable, undeliverable, timeout);
    }

    public DefaultInternalMessage(UUID id, ActorRef sender, ActorRef receiver, ByteBuffer payload, String payloadClass, boolean durable, boolean undeliverable) {
        this(id, sender, ImmutableList.of(receiver), payload, payloadClass, durable, undeliverable, NO_TIMEOUT);
    }

    private DefaultInternalMessage(
            UUID id,
            ActorRef sender,
            ImmutableList<ActorRef> receivers,
            ByteBuffer payload,
            String payloadClass,
            boolean durable,
            boolean undeliverable,
            int timeout) {
        this.sender = sender;
        this.receivers = receivers;
        this.id = id;
        this.payload = payload;
        this.payloadClass = payloadClass;
        this.durable = durable;
        this.undeliverable = undeliverable;
        this.timeout = timeout;
    }

    public DefaultInternalMessage(UUID id,
            ActorRef sender,
            ImmutableList<ActorRef> receivers,
            ByteBuffer payload,
            String payloadClass,
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
        this.payloadClass = payloadClass;
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
        return payloadClass;
    }

    @Nullable
    @Override
    public Class<?> getType() {
        return null;
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
    public <T> T getPayload(MessageDeserializer<T> deserializer) throws IOException {
        //return deserializer.deserialize(payload);
        return SerializationContext.deserialize(deserializer, payload.asReadOnlyBuffer());
    }

    @Override
    public String getPayloadClass() {
        return payloadClass;
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
        return false;
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
        return new DefaultInternalMessage(
                id,
                sender,
                receivers,
                payload.asReadOnlyBuffer(),
                payloadClass,
                durable,
                undeliverable,
                timeout,
                getTraceContext(),
                getCreationContext());
    }

    @Override
    public ImmutableMap<Integer, InternalMessage> splitFor(Function<String, Integer> hashFunction) {
        return receivers.size() <= 1
            ? ImmutableMap.of(calculateHash(receivers, hashFunction), this)
            : groupByReceiverHash(hashFunction);
    }

    private ImmutableMap<Integer, InternalMessage> groupByReceiverHash(Function<String, Integer> hashFunction) {
        Map<Integer, List<ActorRef>> grouped = groupByHashValue(receivers, hashFunction);
        ImmutableMap.Builder<Integer, InternalMessage> builder = ImmutableMap.builder();
        for (Map.Entry<Integer, List<ActorRef>> e : grouped.entrySet()) {
            builder.put(
                e.getKey(),
                new DefaultInternalMessage(
                    UUIDTools.createTimeBasedUUID(),
                    sender,
                    ImmutableList.copyOf(e.getValue()),
                    payload.asReadOnlyBuffer(),
                    payloadClass,
                    durable,
                    undeliverable,
                    timeout,
                    getTraceContext(),
                    getCreationContext()
                )
            );
        }
        return builder.build();
    }
}
