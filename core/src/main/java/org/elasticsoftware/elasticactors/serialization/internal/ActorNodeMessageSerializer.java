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
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.messaging.internal.ActorNodeMessage;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;
import org.elasticsoftware.elasticactors.util.SerializationTools;

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
        Elasticactors.ActorNodeMessage.Builder builder = Elasticactors.ActorNodeMessage.newBuilder();
        builder.setReceiver(ActorRefSerializer.get().serialize(actorNodeMessage.getReceiverRef()));
        builder.setNodeId(actorNodeMessage.getNodeId());
        builder.setPayloadClass(message.getClass().getName());
        MessageSerializer serializer = actorSystems.get(null).getSerializer(message.getClass());
        builder.setPayload(ByteString.copyFrom(serializer.serialize(message)));
        builder.setUndeliverable(actorNodeMessage.isUndeliverable());
        return ByteBuffer.wrap(builder.build().toByteArray());
    }

    private byte[] serializeState(String actorClass,ActorState state) throws IOException {
        try {
            return SerializationTools.serializeActorState(actorSystems,
                                                          (Class<? extends ElasticActor>) Class.forName(actorClass),
                                                          state);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
