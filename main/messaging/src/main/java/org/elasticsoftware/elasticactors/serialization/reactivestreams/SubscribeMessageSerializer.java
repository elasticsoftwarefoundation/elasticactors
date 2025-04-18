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

import org.elasticsoftware.elasticactors.messaging.reactivestreams.SubscribeMessage;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Reactivestreams;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class SubscribeMessageSerializer implements MessageSerializer<SubscribeMessage> {
    @Override
    public ByteBuffer serialize(SubscribeMessage message) throws IOException {
        Reactivestreams.SubscribeMessage.Builder builder = Reactivestreams.SubscribeMessage.newBuilder();
        builder.setMessageName(message.getMessageName());
        builder.setSubscriberRef(message.getSubscriberRef().toString());
        return ByteBuffer.wrap(builder.build().toByteArray());
    }
}
