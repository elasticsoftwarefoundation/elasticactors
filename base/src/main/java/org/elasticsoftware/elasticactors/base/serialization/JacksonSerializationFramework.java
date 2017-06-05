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

package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.serialization.*;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Joost van de Wijgerd
 */
@Named
public final class JacksonSerializationFramework implements SerializationFramework {
    private final ConcurrentMap<TypeVersionPair,JacksonMessageDeserializer> deserializers = new ConcurrentHashMap<>();
    private final JacksonMessageSerializer serializer;
    private final ObjectMapper objectMapper;
    private final JacksonActorStateSerializer actorStateSerializer;
    private final JacksonActorStateDeserializer actorStateDeserializer;

    @Inject
    public JacksonSerializationFramework(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.serializer = new JacksonMessageSerializer(objectMapper);
        this.actorStateSerializer = new JacksonActorStateSerializer(objectMapper);
        this.actorStateDeserializer = new JacksonActorStateDeserializer(objectMapper);
    }

    @Override
    public void register(Class<?> messageClass) {
        Message messageAnnotation;
        if((messageAnnotation = messageClass.getAnnotation(Message.class)) != null
           && this.getClass().equals(messageAnnotation.serializationFramework()))  {
            String messageType = messageAnnotation.type().equals(Message.DEFAULT_TYPE) ? messageClass.getName() : messageAnnotation.type();
            deserializers.putIfAbsent(new TypeVersionPair(messageType, messageAnnotation.version()), new JacksonMessageDeserializer(messageClass,objectMapper));
        }
    }

    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        return serializer;
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(String messageType, String messageVersion) {
        return deserializers.get(new TypeVersionPair(messageType, messageVersion));
    }

    @Override
    public Serializer<ActorState, byte[]> getActorStateSerializer(Class<? extends ElasticActor> actorClass) {
        return actorStateSerializer;
    }

    @Override
    public Serializer<ActorState, byte[]> getActorStateSerializer(ActorState actorState) {
        return actorStateSerializer;
    }

    @Override
    public Deserializer<byte[], ActorState> getActorStateDeserializer(Class<? extends ElasticActor> actorClass) {
        return actorStateDeserializer;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
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
