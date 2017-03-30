/*
 * Copyright 2013 - 2017 The Original Authors
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

package org.elasticsoftware.elasticactors.serialization.reactivestreams;

import org.elasticsoftware.elasticactors.messaging.reactivestreams.CompletedMessage;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Reactivestreams;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class CompletedMessageSerializer implements MessageSerializer<CompletedMessage> {
    @Override
    public ByteBuffer serialize(CompletedMessage message) throws IOException {
        Reactivestreams.CompletedMessage.Builder builder = Reactivestreams.CompletedMessage.newBuilder();
        builder.setMessageName(message.getMessageName());
        return ByteBuffer.wrap(builder.build().toByteArray());
    }
}
