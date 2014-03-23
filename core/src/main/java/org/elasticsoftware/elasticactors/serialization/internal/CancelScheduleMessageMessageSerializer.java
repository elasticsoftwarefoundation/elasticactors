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
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.messaging.internal.CancelScheduledMessageMessage;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class CancelScheduleMessageMessageSerializer implements MessageSerializer<CancelScheduledMessageMessage> {

    public CancelScheduleMessageMessageSerializer() {

    }

    @Override
    public ByteBuffer serialize(CancelScheduledMessageMessage message) throws IOException {
        Elasticactors.CancelScheduledMessageMessage.Builder builder = Elasticactors.CancelScheduledMessageMessage.newBuilder();
        builder.setMessageId(ByteString.copyFrom(UUIDTools.toByteArray(message.getMessageId())));
        builder.setFireTime(message.getFireTime());
        return ByteBuffer.wrap(builder.build().toByteArray());
    }

}
