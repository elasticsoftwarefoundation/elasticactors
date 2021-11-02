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
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.TraceContext;

import javax.annotation.Nullable;
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
public final class TransientInternalMessage extends AbstractTracedMessage
        implements InternalMessage, Serializable, Splittable<String, InternalMessage> {
    private final ActorRef sender;
    private final ImmutableList<ActorRef> receivers;
    private final UUID id;
    private final Object payload;
    private final boolean undeliverable;

    public TransientInternalMessage(ActorRef sender, ActorRef receiver, Object payload) {
        this(sender,receiver,payload,false);
    }

    public TransientInternalMessage(ActorRef sender, ImmutableList<ActorRef> receivers, Object payload) {
        this(sender,receivers,payload,false);
    }

    public TransientInternalMessage(ActorRef sender, ActorRef receiver, Object payload, boolean undeliverable) {
        this(sender, ImmutableList.of(receiver), payload, undeliverable);
    }

    private TransientInternalMessage(
            ActorRef sender,
            ImmutableList<ActorRef> receivers,
            Object payload,
            boolean undeliverable) {
        this.sender = sender;
        this.receivers = receivers;
        this.id = UUIDTools.createTimeBasedUUID();
        this.payload = payload;
        this.undeliverable = undeliverable;
    }

    public TransientInternalMessage(
            ActorRef sender,
            ImmutableList<ActorRef> receivers,
            Object payload,
            boolean undeliverable,
            TraceContext traceContext,
            CreationContext creationContext) {
        super(traceContext, creationContext);
        this.sender = sender;
        this.receivers = receivers;
        this.id = UUIDTools.createTimeBasedUUID();
        this.payload = payload;
        this.undeliverable = undeliverable;
    }

    @Override
    @Nullable
    public ActorRef getSender() {
        return sender;
    }

    @Override
    public String getTypeAsString() {
        return payload.getClass().getName();
    }

    @Nullable
    @Override
    public Class<?> getType() {
        return payload.getClass();
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
        throw new UnsupportedOperationException(String.format(
            "This implementation is intended to be used local only, for remote use [%s]",
            DefaultInternalMessage.class.getSimpleName()
        ));
    }

    @Override
    public <T> T getPayload(MessageDeserializer<T> deserializer) {
        return (T) payload;
    }

    @Override
    public String getPayloadClass() {
        return payload.getClass().getName();
    }

    @Override
    public boolean isDurable() {
        return false;
    }

    @Override
    public boolean isUndeliverable() {
        return undeliverable;
    }

    @Override
    public int getTimeout() {
        return NO_TIMEOUT;
    }

    @Override
    public boolean hasSerializedPayload() {
        return false;
    }

    @Override
    public boolean hasPayloadObject() {
        return payload != null;
    }

    @Override
    public byte[] toByteArray() {
        throw new UnsupportedOperationException(String.format(
            "This implementation is intended to be used local only, for remote use [%s]",
            DefaultInternalMessage.class.getSimpleName()));
    }

    @Override
    public InternalMessage copyOf() {
        return this;
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
                new TransientInternalMessage(
                    sender,
                    ImmutableList.copyOf(e.getValue()),
                    payload,
                    undeliverable,
                    getTraceContext(),
                    getCreationContext()
                )
            );
        }
        return builder.build();
    }
}
