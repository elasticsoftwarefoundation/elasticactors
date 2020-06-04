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
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.tracing.RealSenderData;
import org.elasticsoftware.elasticactors.tracing.TraceData;
import org.elasticsoftware.elasticactors.tracing.TraceDataHolder;
import org.elasticsoftware.elasticactors.util.TracingHelper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public final class TransientInternalMessage implements InternalMessage,Serializable {
    private final ActorRef sender;
    private final ImmutableList<ActorRef> receivers;
    private final UUID id;
    private final Object payload;
    private final boolean undeliverable;
    private final RealSenderData realSenderData;
    private final TraceData traceData;

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
        this(
                sender,
                receivers,
                payload,
                undeliverable,
                TracingHelper.findRealSender(),
                new TraceData(TraceDataHolder.currentTraceData()));
    }

    public TransientInternalMessage(
            ActorRef sender,
            ImmutableList<ActorRef> receivers,
            Object payload,
            boolean undeliverable,
            RealSenderData realSenderData,
            TraceData traceData) {
        this.sender = sender;
        this.receivers = receivers;
        this.id = UUIDTools.createTimeBasedUUID();
        this.payload = payload;
        this.undeliverable = undeliverable;
        this.realSenderData = realSenderData;
        this.traceData = traceData;
    }

    public ActorRef getSender() {
        return sender;
    }

    @Override
    public ImmutableList<ActorRef> getReceivers() {
        return receivers;
    }

    public UUID getId() {
        return id;
    }

    public ByteBuffer getPayload() {
        throw new UnsupportedOperationException(String.format("This implementation is intended to be used local only, for remote use [%s]",InternalMessageImpl.class.getSimpleName()));
    }

    @Override
    public <T> T getPayload(MessageDeserializer<T> deserializer) throws IOException {
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
    public byte[] toByteArray() {
        throw new UnsupportedOperationException(String.format("This implementation is intended to be used local only, for remote use [%s]",InternalMessageImpl.class.getSimpleName()));
    }

    @Override
    public InternalMessage copyOf() {
        return this;
    }

    @Nullable
    @Override
    public RealSenderData getRealSenderData() {
        return realSenderData;
    }

    @Nullable
    @Override
    public TraceData getTraceData() {
        return traceData;
    }
}
