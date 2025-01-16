/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.serialization.internal;

import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.util.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class DestroyActorMessageDeserializer implements MessageDeserializer<DestroyActorMessage> {
    private final ActorRefDeserializer actorRefDeserializer;

    public DestroyActorMessageDeserializer(ActorRefDeserializer actorRefDeserializer) {
        this.actorRefDeserializer = actorRefDeserializer;
    }

    @Override
    public DestroyActorMessage deserialize(ByteBuffer serializedObject) throws IOException {
        Messaging.DestroyActorMessage protobufMessage = ByteBufferUtils.throwingApplyAndReset(
            serializedObject,
            Messaging.DestroyActorMessage::parseFrom
        );
        return new DestroyActorMessage(actorRefDeserializer.deserialize(protobufMessage.getActorRef()));
    }

    @Override
    public Class<DestroyActorMessage> getMessageClass() {
        return DestroyActorMessage.class;
    }

    @Override
    public boolean isSafe() {
        return true;
    }
}
