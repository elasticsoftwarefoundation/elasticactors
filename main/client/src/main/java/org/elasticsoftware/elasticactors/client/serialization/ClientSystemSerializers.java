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

package org.elasticsoftware.elasticactors.client.serialization;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.messaging.internal.ActivateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.CancelScheduledMessageMessage;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;
import org.elasticsoftware.elasticactors.serialization.SystemSerializers;
import org.elasticsoftware.elasticactors.serialization.internal.ActivateActorMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.internal.CancelScheduleMessageMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.internal.CreateActorMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.internal.DestroyActorMessageSerializer;

public final class ClientSystemSerializers implements SystemSerializers {

    private final ImmutableMap<Class, MessageSerializer> systemSerializers;

    public ClientSystemSerializers(SerializationFrameworks serializationFrameworks) {
        ImmutableMap.Builder<Class, MessageSerializer> builder = ImmutableMap.builder();
        builder.put(CreateActorMessage.class, new CreateActorMessageSerializer(serializationFrameworks));
        builder.put(DestroyActorMessage.class, new DestroyActorMessageSerializer());
        builder.put(ActivateActorMessage.class, new ActivateActorMessageSerializer());
        builder.put(CancelScheduledMessageMessage.class, new CancelScheduleMessageMessageSerializer());
        this.systemSerializers = builder.build();
    }

    @Override
    public <T> MessageSerializer<T> get(Class<T> messageClass) {
        return systemSerializers.get(messageClass);
    }

}
