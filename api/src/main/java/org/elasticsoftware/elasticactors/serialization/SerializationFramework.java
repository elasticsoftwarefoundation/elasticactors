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

package org.elasticsoftware.elasticactors.serialization;

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;

/**
 * @author Joost van de Wijgerd
 */
public interface SerializationFramework {
    /**
     * Register a message class with the framework
     *
     * @param messageClass the message class to be registered
     */
    void register(Class<?> messageClass);

    /**
     * Return the correct serializer for the passed in message class
     *
     * @param messageClass the message class for which to get the serializer
     * @param <T> the message class for which to get the serializer
     * @return the correct serializer for the given message class
     */
    <T> MessageSerializer<T> getSerializer(Class<T> messageClass);

    /**
     * Return the correct String serializer for the passed message class
     *
     * @param messageClass the message class for which to get the serializer
     * @param <T> the message class for which to get the serializer
     * @return the correct String serializer for the given message class
     */
    <T> MessageToStringSerializer<T> getToStringSerializer(Class<T> messageClass);

    /**
     * Return the correct deserializer for the specified message class
     *
     * @param messageClass the message class for which to get the deserializer
     * @param <T> the message class for which to get the serializer
     * @return the correct deserializer for the given message class
     */
    <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass);

    /**
     * Return the serializer for the actor state
     *
     * @param actorClass the actor class for which to get the serializer
     * @return the correct serializer for the given actor's state class
     */
    Serializer<ActorState, byte[]> getActorStateSerializer(Class<? extends ElasticActor> actorClass);

    /**
     * Return the serializer for the actor state
     *
     * @param actorState the state object for which to get a serializer
     * @return the correct serializer for the given actor state object's type
     */
    Serializer<ActorState, byte[]> getActorStateSerializer(ActorState actorState);

    /**
     * Return the deserializer for the actor state
     *
     * @param actorClass the actor class for which to get the deserializer
     * @return the correct deserializer for the given actor's state class
     */
    Deserializer<byte[], ActorState> getActorStateDeserializer(Class<? extends ElasticActor> actorClass);
}
