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

import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.messaging.internal.CancelScheduledMessageMessage;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Messaging;
import org.elasticsoftware.elasticactors.util.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class CancelScheduledMessageMessageDeserializer implements MessageDeserializer<CancelScheduledMessageMessage> {


    public CancelScheduledMessageMessageDeserializer() {

    }

    @Override
    public CancelScheduledMessageMessage deserialize(ByteBuffer serializedObject) throws IOException {
        Messaging.CancelScheduledMessageMessage protobufMessage =
            ByteBufferUtils.throwingApplyAndReset(
                serializedObject,
                Messaging.CancelScheduledMessageMessage::parseFrom
            );
        return new CancelScheduledMessageMessage(
            UUIDTools.fromByteString(protobufMessage.getMessageId()),
            protobufMessage.getFireTime()
        );
    }

    @Override
    public Class<CancelScheduledMessageMessage> getMessageClass() {
        return CancelScheduledMessageMessage.class;
    }

    @Override
    public boolean isSafe() {
        return true;
    }
}
