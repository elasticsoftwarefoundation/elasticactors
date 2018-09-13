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

package org.elasticsoftware.elasticactors.serialization;

import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.cluster.MessageSerializationRegistry;
import org.elasticsoftware.elasticactors.cluster.SerializationFrameworkRegistry;
import org.elasticsoftware.elasticactors.messaging.internal.*;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.*;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.internal.*;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public final class SystemSerializers {
    private final Map<Class,MessageSerializer> systemSerializers = new HashMap<Class,MessageSerializer>();

    public SystemSerializers(SerializationFrameworkRegistry serializationFrameworkRegistry,
                             MessageSerializationRegistry messageSerializationRegistry) {
        systemSerializers.put(CreateActorMessage.class,new CreateActorMessageSerializer(serializationFrameworkRegistry));
        systemSerializers.put(DestroyActorMessage.class,new DestroyActorMessageSerializer());
        systemSerializers.put(ActivateActorMessage.class,new ActivateActorMessageSerializer());
        systemSerializers.put(CancelScheduledMessageMessage.class,new CancelScheduleMessageMessageSerializer());
        systemSerializers.put(ActorNodeMessage.class, new ActorNodeMessageSerializer(messageSerializationRegistry));
        systemSerializers.put(PersistActorMessage.class, new PersistActorMessageSerializer());
        //reactive streams protocol
        systemSerializers.put(CancelMessage.class, new CancelMessageSerializer());
        systemSerializers.put(CompletedMessage.class, new CompletedMessageSerializer());
        systemSerializers.put(NextMessage.class, new NextMessageSerializer());
        systemSerializers.put(RequestMessage.class, new RequestMessageSerializer());
        systemSerializers.put(SubscribeMessage.class, new SubscribeMessageSerializer());
        systemSerializers.put(SubscriptionMessage.class, new SubscriptionMessageSerializer());
    }

    public <T> MessageSerializer<T> get(Class<T> messageClass) {
        return systemSerializers.get(messageClass);
    }
}
