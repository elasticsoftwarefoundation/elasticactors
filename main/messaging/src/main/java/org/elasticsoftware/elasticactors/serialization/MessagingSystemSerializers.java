/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.serialization;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.messaging.internal.ActivateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.ActorNodeMessage;
import org.elasticsoftware.elasticactors.messaging.internal.CancelScheduledMessageMessage;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.PersistActorMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.CancelMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.CompletedMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.NextMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.RequestMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.SubscribeMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.SubscriptionMessage;
import org.elasticsoftware.elasticactors.serialization.internal.ActivateActorMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.internal.ActorNodeMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.internal.CancelScheduleMessageMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.internal.CreateActorMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.internal.DestroyActorMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.internal.PersistActorMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.CancelMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.CompletedMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.NextMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.RequestMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.SubscribeMessageSerializer;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.SubscriptionMessageSerializer;

/**
 * @author Joost van de Wijgerd
 */
public final class MessagingSystemSerializers implements SystemSerializers {

    private final ImmutableMap<Class, MessageSerializer> systemSerializers;

    public MessagingSystemSerializers(InternalActorSystems cluster) {
        ImmutableMap.Builder<Class, MessageSerializer> builder = ImmutableMap.builder();
        builder.put(CreateActorMessage.class,new CreateActorMessageSerializer(cluster));
        builder.put(DestroyActorMessage.class,new DestroyActorMessageSerializer());
        builder.put(ActivateActorMessage.class,new ActivateActorMessageSerializer());
        builder.put(CancelScheduledMessageMessage.class,new CancelScheduleMessageMessageSerializer());
        builder.put(ActorNodeMessage.class, new ActorNodeMessageSerializer(cluster));
        builder.put(PersistActorMessage.class, new PersistActorMessageSerializer());
        //reactive streams protocol
        builder.put(CancelMessage.class, new CancelMessageSerializer());
        builder.put(CompletedMessage.class, new CompletedMessageSerializer());
        builder.put(NextMessage.class, new NextMessageSerializer());
        builder.put(RequestMessage.class, new RequestMessageSerializer());
        builder.put(SubscribeMessage.class, new SubscribeMessageSerializer());
        builder.put(SubscriptionMessage.class, new SubscriptionMessageSerializer());
        this.systemSerializers = builder.build();
    }

    @Override
    public <T> MessageSerializer<T> get(Class<T> messageClass) {
        return systemSerializers.get(messageClass);
    }
}
