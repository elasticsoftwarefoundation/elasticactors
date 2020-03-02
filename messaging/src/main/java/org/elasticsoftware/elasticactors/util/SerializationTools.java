/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.util;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageStringSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationAccessor;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class SerializationTools {

    public static Object deserializeMessage(
            SerializationAccessor serializationAccessor,
            InternalMessage internalMessage) throws Exception {
        return deserializeMessage(
                serializationAccessor,
                Class.forName(internalMessage.getPayloadClass()),
                internalMessage);
    }

    public static Object deserializeMessage(
            SerializationAccessor serializationAccessor,
            Class<?> messageClass,
            InternalMessage internalMessage) throws Exception {
        MessageDeserializer<?> deserializer = serializationAccessor.getDeserializer(messageClass);

        if (deserializer != null) {
            return internalMessage.getPayload(deserializer);
        } else {
            //@todo: throw a more targeted exception
            throw new Exception(String.format(
                    "No Deserializer found for Message class %s",
                    internalMessage.getPayloadClass()));
        }

    }

    public static <T> MessageStringSerializer<T> getStringSerializer(
            SerializationFrameworks serializationFrameworks,
            Class<T> messageClass) {
        Message messageAnnotation = messageClass.getAnnotation(Message.class);
        if (messageAnnotation != null) {
            SerializationFramework serializationFramework = serializationFrameworks
                    .getSerializationFramework(messageAnnotation.serializationFramework());
            if (serializationFramework != null) {
                return serializationFramework.getStringSerializer(messageClass);
            }
        }
        return null;
    }

    public static ActorState deserializeActorState(SerializationFrameworks serializationFrameworks, Class<? extends ElasticActor> actorClass, byte[] serializedState) throws IOException {
        Actor actorAnnotation = actorClass.getAnnotation(Actor.class);
        if(actorAnnotation != null) {
            SerializationFramework framework = serializationFrameworks.getSerializationFramework(actorAnnotation.serializationFramework());
            return framework.getActorStateDeserializer(actorClass).deserialize(serializedState);
        } else {
            // @todo: what to do if we can't determine the state?
            return null;
        }
    }

    public static byte[] serializeActorState(SerializationFrameworks serializationFrameworks, Class<? extends ElasticActor> actorClass,ActorState actorState) throws IOException {
        Actor actorAnnotation = actorClass.getAnnotation(Actor.class);
        if(actorAnnotation != null) {
            SerializationFramework framework = serializationFrameworks.getSerializationFramework(actorAnnotation.serializationFramework());
            return framework.getActorStateSerializer(actorClass).serialize(actorState);
        } else {
            // @todo: what to do if we can't determine the state?
            return null;
        }
    }

    public static byte[] serializeActorState(SerializationFrameworks serializationFrameworks, ActorState actorState) throws IOException {
        SerializationFramework framework = serializationFrameworks.getSerializationFramework(actorState.getSerializationFramework());
        return framework.getActorStateSerializer(actorState).serialize(actorState);
    }

    public static SerializationFramework getSerializationFramework(SerializationFrameworks serializationFrameworks, Class<? extends ElasticActor> actorClass) {
        Actor actorAnnotation = actorClass.getAnnotation(Actor.class);
        if(actorAnnotation != null) {
            return serializationFrameworks.getSerializationFramework(actorAnnotation.serializationFramework());
        } else {
            return null;
        }
    }
}
