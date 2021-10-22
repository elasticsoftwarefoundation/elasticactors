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

package org.elasticsoftware.elasticactors.serialization.internal;

import com.google.protobuf.ByteString;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.messaging.internal.ActorNodeMessage;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class ActorNodeMessageSerializer implements MessageSerializer<ActorNodeMessage> {
    private final InternalActorSystems actorSystems;

    public ActorNodeMessageSerializer(InternalActorSystems actorSystems) {
        this.actorSystems = actorSystems;
    }

    @Override
    public ByteBuffer serialize(ActorNodeMessage actorNodeMessage) throws IOException {
        Object message = actorNodeMessage.getMessage();
        Messaging.ActorNodeMessage.Builder builder = Messaging.ActorNodeMessage.newBuilder();
        builder.setReceiver(ActorRefSerializer.get().serialize(actorNodeMessage.getReceiverRef()));
        builder.setNodeId(actorNodeMessage.getNodeId());
        builder.setPayloadClass(message.getClass().getName());
        MessageSerializer serializer = actorSystems.get(null).getSerializer(message.getClass());
        builder.setPayload(ByteString.copyFrom(serializer.serialize(message)));
        builder.setUndeliverable(actorNodeMessage.isUndeliverable());
        return ByteBuffer.wrap(builder.build().toByteArray());
    }
}
