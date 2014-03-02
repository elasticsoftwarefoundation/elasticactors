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

package org.elasticsoftware.elasticactors.util;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class SerializationTools {
    public static Object deserializeMessage(InternalActorSystem actorSystem,InternalMessage internalMessage) throws Exception {
        Class<?> messageClass = Class.forName(internalMessage.getPayloadClass());
        MessageDeserializer<?> deserializer = actorSystem.getDeserializer(messageClass);

        if(deserializer != null) {
            return internalMessage.getPayload(deserializer);
        } else {
            //@todo: throw a more targeted exception
            throw new Exception(String.format("No Deserializer found for Message class %s in ActorSystem [%s]",
                                              internalMessage.getPayloadClass(),actorSystem.getName()));
        }

    }

    public static ActorState deserializeActorState(InternalActorSystems actorSystems,Class<? extends ElasticActor> actorClass, byte[] serializedState) throws IOException {
        Actor actorAnnotation = actorClass.getAnnotation(Actor.class);
        if(actorAnnotation != null) {
            SerializationFramework framework = actorSystems.getSerializationFramework(actorAnnotation.serializationFramework());
            return framework.getActorStateDeserializer(actorClass).deserialize(serializedState);
        } else {
            // @todo: what to do if we can't determine the state?
            return null;
        }
    }

    public static byte[] serializeActorState(InternalActorSystems actorSystems,Class<? extends ElasticActor> actorClass,ActorState actorState) throws IOException {
        Actor actorAnnotation = actorClass.getAnnotation(Actor.class);
        if(actorAnnotation != null) {
            SerializationFramework framework = actorSystems.getSerializationFramework(actorAnnotation.serializationFramework());
            return framework.getActorStateSerializer(actorClass).serialize(actorState);
        } else {
            // @todo: what to do if we can't determine the state?
            return null;
        }
    }
}
