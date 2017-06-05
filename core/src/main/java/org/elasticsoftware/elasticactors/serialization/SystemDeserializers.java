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

import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.messaging.internal.*;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.*;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.*;
import org.elasticsoftware.elasticactors.serialization.reactivestreams.*;
import org.elasticsoftware.elasticactors.util.MessageTools;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public final class SystemDeserializers {
    private final Map<TypeVersionPair,MessageDeserializer> systemDeserializers = new HashMap<>();

    public SystemDeserializers(InternalActorSystems cluster,ActorRefFactory actorRefFactory) {
        ActorRefDeserializer actorRefDeserializer = new ActorRefDeserializer(actorRefFactory);
        register(CreateActorMessage.class,new CreateActorMessageDeserializer(cluster));
        register(DestroyActorMessage.class,new DestroyActorMessageDeserializer(actorRefDeserializer));
        register(ActivateActorMessage.class,new ActivateActorMessageDeserializer());
        register(CancelScheduledMessageMessage.class,new CancelScheduledMessageMessageDeserializer());
        register(ActorNodeMessage.class, new ActorNodeMessageDeserializer(actorRefDeserializer, cluster));
        // reactive streams protocol
        register(CancelMessage.class, new CancelMessageDeserializer(actorRefDeserializer));
        register(CompletedMessage.class, new CompletedMessageDeserializer());
        register(SubscribeMessage.class, new SubscribeMessageDeserializer(actorRefDeserializer));
        register(RequestMessage.class, new RequestMessageDeserializer());
        register(SubscriptionMessage.class, new SubscriptionMessageDeserializer());
        register(NextMessage.class, new NextMessageDeserializer());

    }

    private void register(Class<?> messageClass, MessageDeserializer messageDeserializer) {
        Message messageAnnotation = messageClass.getAnnotation(Message.class);
        String messageType = messageAnnotation == null ? messageClass.getName() :
                (messageAnnotation.type().equals(Message.DEFAULT_TYPE) ? messageClass.getName() : messageAnnotation.type());
        String messageVersion = messageAnnotation == null ? "1" : messageAnnotation.version();
        systemDeserializers.put(new TypeVersionPair(messageType, messageVersion), messageDeserializer);
        MessageTools.register(messageClass);
    }

    public <T> MessageDeserializer<T> get(Class<T> messageClass) {
        Message messageAnnotation = messageClass.getAnnotation(Message.class);
        String messageType = messageAnnotation == null ? messageClass.getName() :
                (messageAnnotation.type().equals(Message.DEFAULT_TYPE) ? messageClass.getName() : messageAnnotation.type());
        String messageVersion = messageAnnotation == null ? "1" : messageAnnotation.version();
        return get(messageType, messageVersion);
    }

    public <T> MessageDeserializer<T> get(String messageType, String messageVersion) {
        return systemDeserializers.get(new TypeVersionPair(messageType, messageVersion));
    }

    private static final class TypeVersionPair {
        private final String type;
        private final String version;

        TypeVersionPair(String type, String version) {
            this.type = type;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TypeVersionPair that = (TypeVersionPair) o;

            if (!type.equals(that.type)) return false;
            return version.equals(that.version);
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + version.hashCode();
            return result;
        }
    }
}
