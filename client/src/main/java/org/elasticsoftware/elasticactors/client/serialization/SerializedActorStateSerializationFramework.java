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


package org.elasticsoftware.elasticactors.client.serialization;

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.client.state.SerializedActorState;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Serializer;

/**
 * This framework acts as a proxy for getting a {@link SerializedActorState}'s byte array and
 * passing it to the {@link org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage}
 */
public final class SerializedActorStateSerializationFramework implements SerializationFramework {

    private final SerializedActorStateSerializer serializer = new SerializedActorStateSerializer();

    @Override
    public void register(Class<?> messageClass) {
        // Do nothing
    }

    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        throw new UnsupportedOperationException("Only actor state serialization is supported");
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        throw new UnsupportedOperationException("Only actor state serialization is supported");
    }

    @Override
    public Serializer<ActorState, byte[]> getActorStateSerializer(Class<? extends ElasticActor> actorClass) {
        if (SerializedActorState.class.isAssignableFrom(actorClass)) {
            return serializer;
        }
        throw new IllegalArgumentException(String.format(
                "Class %s not supported by this framework",
                actorClass.getName()));
    }

    @Override
    public Serializer<ActorState, byte[]> getActorStateSerializer(ActorState actorState) {
        if (actorState instanceof SerializedActorState) {
            return serializer;
        }
        throw new IllegalArgumentException(String.format(
                "Class %s not supported by this framework",
                actorState.getClass().getName()));
    }

    @Override
    public Deserializer<byte[], ActorState> getActorStateDeserializer(Class<?
            extends ElasticActor> actorClass) {
        throw new UnsupportedOperationException("Only actor state serialization is supported");
    }
}
