/*
 * Copyright 2013 - 2023 The Original Authors
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

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;

import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class SystemSerializationFramework implements SerializationFramework {
    private final SerializationFrameworks serializationFrameworks;

    public SystemSerializationFramework(SerializationFrameworks serializationFrameworks) {
        this.serializationFrameworks = serializationFrameworks;
    }

    @Override
    public void register(Class<?> messageClass) {
        // do nothing, classes are already registered
    }

    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        return serializationFrameworks.getSystemMessageSerializer(messageClass);
    }

    @Override
    public MessageToStringConverter getToStringConverter() {
        return null;
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        return serializationFrameworks.getSystemMessageDeserializer(messageClass);
    }

    @Override
    public Serializer<ActorState, byte[]> getActorStateSerializer(Class<? extends ElasticActor> actorClass) {
        throw new UnsupportedOperationException("Only message serialization and deserialization supported");
    }

    @Override
    public Serializer<ActorState, byte[]> getActorStateSerializer(ActorState actorState) {
        throw new UnsupportedOperationException("Only message serialization and deserialization supported");
    }

    @Override
    public Deserializer<ByteBuffer, ActorState> getActorStateDeserializer(Class<? extends ElasticActor> actorClass) {
        throw new UnsupportedOperationException("Only message serialization and deserialization supported");
    }
}
