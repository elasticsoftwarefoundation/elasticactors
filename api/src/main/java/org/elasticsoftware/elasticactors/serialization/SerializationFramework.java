/*
 * Copyright 2013 the original authors
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

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;

/**
 * @author Joost van de Wijgerd
 */
public interface SerializationFramework {
    /**
     * Register a message class with the framework
     *
     * @param messageClass
     */
    void register(Class<?> messageClass);

    /**
     * Return the correct serializer for the passed in message class
     *
     * @param messageClass
     * @param <T>
     * @return
     */
    <T> MessageSerializer<T> getSerializer(Class<T> messageClass);

    /**
     * Return the correct deserializer for the specified message class
     *
     * @param messageClass
     * @param <T>
     * @return
     */
    <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass);

    /**
     * Return the serializer for the actor state
     *
     * @param actorClass
     * @return
     */
    Serializer<ActorState, byte[]> getActorStateSerializer(Class<? extends ElasticActor> actorClass);

    /**
     * Return the deserializer for the specified actor class
     *
     * @param actorClass
     * @return
     */
    Deserializer<byte[], ActorState> getActorStateDeserializer(Class<? extends ElasticActor> actorClass);
}
