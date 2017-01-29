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

package org.elasticsoftware.elasticactors.serialization.internal;

import com.google.protobuf.ByteString;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;

import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public final class ScheduledMessageSerializer implements Serializer<ScheduledMessage,byte[]> {
    private static final ScheduledMessageSerializer INSTANCE = new ScheduledMessageSerializer();

    public static ScheduledMessageSerializer get() {
        return INSTANCE;
    }

    @Override
    public byte[] serialize(ScheduledMessage scheduledMessage) {
        Elasticactors.ScheduledMessage.Builder builder = Elasticactors.ScheduledMessage.newBuilder();
        builder.setId(ByteString.copyFrom(UUIDTools.toByteArray(scheduledMessage.getId())));
        builder.setFireTime(scheduledMessage.getFireTime(TimeUnit.MILLISECONDS));
        builder.setMessageClass(scheduledMessage.getMessageClass().getName());
        builder.setSender(scheduledMessage.getSender().toString());
        builder.setReceiver(scheduledMessage.getReceiver().toString());
        builder.setMessage(ByteString.copyFrom(scheduledMessage.getMessageBytes()));
        return builder.build().toByteArray();
    }


}
