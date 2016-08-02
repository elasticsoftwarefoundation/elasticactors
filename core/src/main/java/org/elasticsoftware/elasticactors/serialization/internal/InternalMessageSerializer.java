/*
 * Copyright 2013 - 2014 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.serialization.internal;

import com.google.protobuf.ByteString;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;

/**
 * @author Joost van de Wijgerd
 */
public final class InternalMessageSerializer implements Serializer<InternalMessage,byte[]> {
    private static final InternalMessageSerializer INSTANCE = new InternalMessageSerializer();

    public static InternalMessageSerializer get() {
        return INSTANCE;
    }

    @Override
    public byte[] serialize(InternalMessage internalMessage) {
        Elasticactors.InternalMessage.Builder builder = Elasticactors.InternalMessage.newBuilder();
        builder.setId(ByteString.copyFrom(UUIDTools.toByteArray(internalMessage.getId())));
        builder.setPayload(ByteString.copyFrom(internalMessage.getPayload()));
        // rewind the payload to not fuck up the internal message
        internalMessage.getPayload().rewind(); // @todo: this is a bit ugly
        builder.setPayloadClass(internalMessage.getPayloadClass());
        // backwards compatibility for single receiver messages (needed when running mixed clusters < 0.24)
        if(internalMessage.getReceivers().size() == 1) {
            builder.setReceiver(ActorRefSerializer.get().serialize(internalMessage.getReceivers().get(0)));
        } else {
            for (ActorRef receiver : internalMessage.getReceivers()) {
                builder.addReceivers(ActorRefSerializer.get().serialize(receiver));
            }
        }
        if(internalMessage.getSender() != null) {
            builder.setSender(ActorRefSerializer.get().serialize(internalMessage.getSender()));
        }
        builder.setDurable(internalMessage.isDurable());
        builder.setUndeliverable(internalMessage.isUndeliverable());
        builder.setTimeout(internalMessage.getTimeout());
        return builder.build().toByteArray();
    }


}
