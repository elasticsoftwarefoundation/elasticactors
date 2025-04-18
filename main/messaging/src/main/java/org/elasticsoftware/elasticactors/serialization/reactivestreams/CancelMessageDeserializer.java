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

package org.elasticsoftware.elasticactors.serialization.reactivestreams;

import org.elasticsoftware.elasticactors.messaging.reactivestreams.CancelMessage;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.ActorRefDeserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Reactivestreams;
import org.elasticsoftware.elasticactors.util.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class CancelMessageDeserializer implements MessageDeserializer<CancelMessage> {
    private final ActorRefDeserializer actorRefDeserializer;

    public CancelMessageDeserializer(ActorRefDeserializer actorRefDeserializer) {
        this.actorRefDeserializer = actorRefDeserializer;
    }

    @Override
    public CancelMessage deserialize(ByteBuffer serializedObject) throws IOException {
        Reactivestreams.CancelMessage cancelMessage = ByteBufferUtils.throwingApplyAndReset(
            serializedObject,
            Reactivestreams.CancelMessage::parseFrom
        );
        return new CancelMessage(actorRefDeserializer.deserialize(cancelMessage.getSubscriberRef()), cancelMessage.getMessageName());
    }

    @Override
    public Class<CancelMessage> getMessageClass() {
        return CancelMessage.class;
    }

    @Override
    public boolean isSafe() {
        return true;
    }
}
