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

package org.elasticsoftware.elasticactors.serialization.reactivestreams;

import com.google.protobuf.ByteString;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.SubscribeMessage;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.ActorRefDeserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Reactivestreams;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class SubscribeMessageDeserializer implements MessageDeserializer<SubscribeMessage> {
    private final ActorRefDeserializer actorRefDeserializer;

    public SubscribeMessageDeserializer(ActorRefDeserializer actorRefDeserializer) {
        this.actorRefDeserializer = actorRefDeserializer;
    }

    @Override
    public SubscribeMessage deserialize(ByteBuffer serializedObject) throws IOException {
        Reactivestreams.SubscribeMessage subscribeMessage = Reactivestreams.SubscribeMessage.parseFrom(ByteString.copyFrom(serializedObject));
        return new SubscribeMessage(actorRefDeserializer.deserialize(subscribeMessage.getSubscriberRef()), subscribeMessage.getMessageName());
    }

    @Override
    public Class<SubscribeMessage> getMessageClass() {
        return SubscribeMessage.class;
    }
}
