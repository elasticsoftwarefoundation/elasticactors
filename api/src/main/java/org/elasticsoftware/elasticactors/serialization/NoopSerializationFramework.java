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
 * Default Serialization Framework. Does nothing (no-operation)
 *
 * @author Leonard Wolters
 */
public final class NoopSerializationFramework implements SerializationFramework {

    @Override
    public void register(Class<?> messageClass) {
    }

    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        return null;
    }

    @Override
    public <T> MessageToStringSerializer<T> getToStringSerializer(Class<T> messageClass) {
        return null;
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        return null;
    }

    @Override
    public Serializer<ActorState, byte[]> getActorStateSerializer(Class<? extends ElasticActor> actorClass) {
        return null;
    }

    @Override
    public Serializer<ActorState, byte[]> getActorStateSerializer(ActorState actorState) {
        return null;
    }

    @Override
    public Deserializer<byte[], ActorState> getActorStateDeserializer(Class<? extends ElasticActor> actorClass) {
        return null;
    }
}
