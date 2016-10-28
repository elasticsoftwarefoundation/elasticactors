/*
 * Copyright 2013 - 2016 The Original Authors
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
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;
import org.elasticsoftware.elasticactors.util.SerializationTools;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class CreateActorMessageSerializer implements MessageSerializer<CreateActorMessage> {
    private final InternalActorSystems actorSystems;

    public CreateActorMessageSerializer(InternalActorSystems actorSystems) {
        this.actorSystems = actorSystems;
    }


    @Override
    public ByteBuffer serialize(CreateActorMessage message) throws IOException {
        Elasticactors.CreateActorMessage.Builder builder = Elasticactors.CreateActorMessage.newBuilder();
        builder.setActorSystem(message.getActorSystem());
        builder.setActorClass(message.getActorClass());
        builder.setActorId(message.getActorId());
        if(message.getInitialState() != null) {
            builder.setInitialState(ByteString.copyFrom(serializeState(message.getActorClass(),
                                                                       message.getInitialState())));
        }
        builder.setType(Elasticactors.ActorType.valueOf(message.getType().ordinal()));
        return ByteBuffer.wrap(builder.build().toByteArray());
    }

    private byte[] serializeState(String actorClass,ActorState state) throws IOException {
        return SerializationTools.serializeActorState(actorSystems, state);
        /**
         * to support creating actors on remote actor systems (without having to link all the code) we are now
         * using the SerializationFramework as it is specified in the ActorState implementation instead of on the
         * Actor annotation
        try {
            return SerializationTools.serializeActorState(actorSystems,
                                                          (Class<? extends ElasticActor>) Class.forName(actorClass),
                                                          state);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        } */
    }
}
