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

package org.elasticsoftware.elasticactors.serialization;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
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
import org.elasticsoftware.elasticactors.serialization.internal.ActivateActorMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.ActorNodeMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.ActorRefDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.CancelScheduledMessageMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.CreateActorMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.DestroyActorMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.PersistActorMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.CancelMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.CompletedMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.NextMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.RequestMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.SubscribeMessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.SubscriptionMessageDeserializer;

/**
 * @author Joost van de Wijgerd
 */
public final class MessagingSystemDeserializers implements SystemDeserializers {

    private final ImmutableMap<Class, MessageDeserializer> systemDeserializers;

    public MessagingSystemDeserializers(InternalActorSystems cluster,ActorRefFactory actorRefFactory) {
        ImmutableMap.Builder<Class, MessageDeserializer> builder = ImmutableMap.builder();
        ActorRefDeserializer actorRefDeserializer = new ActorRefDeserializer(actorRefFactory);
        builder.put(CreateActorMessage.class,new CreateActorMessageDeserializer(cluster));
        builder.put(DestroyActorMessage.class,new DestroyActorMessageDeserializer(actorRefDeserializer));
        builder.put(ActivateActorMessage.class,new ActivateActorMessageDeserializer());
        builder.put(CancelScheduledMessageMessage.class,new CancelScheduledMessageMessageDeserializer());
        builder.put(ActorNodeMessage.class, new ActorNodeMessageDeserializer(actorRefDeserializer, cluster));
        builder.put(PersistActorMessage.class, new PersistActorMessageDeserializer(actorRefDeserializer));
        // reactive streams protocol
        builder.put(CancelMessage.class, new CancelMessageDeserializer(actorRefDeserializer));
        builder.put(CompletedMessage.class, new CompletedMessageDeserializer());
        builder.put(SubscribeMessage.class, new SubscribeMessageDeserializer(actorRefDeserializer));
        builder.put(RequestMessage.class, new RequestMessageDeserializer());
        builder.put(SubscriptionMessage.class, new SubscriptionMessageDeserializer());
        builder.put(NextMessage.class, new NextMessageDeserializer());
        this.systemDeserializers = builder.build();
    }

    @Override
    public <T> MessageDeserializer<T> get(Class<T> messageClass) {
        return systemDeserializers.get(messageClass);
    }
}
