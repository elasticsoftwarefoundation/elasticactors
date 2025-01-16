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
import org.elasticsoftware.elasticactors.messaging.internal.*;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.*;
import org.elasticsoftware.elasticactors.serialization.internal.*;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.*;

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
