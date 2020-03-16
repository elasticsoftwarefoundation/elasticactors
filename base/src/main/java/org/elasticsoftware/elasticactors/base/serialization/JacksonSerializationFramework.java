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

package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.MessageToStringSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.springframework.beans.factory.annotation.Value;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Joost van de Wijgerd
 */
@Named
public final class JacksonSerializationFramework implements SerializationFramework {

    public final static int DEFAULT_MAX_LENGTH = 5_000;

    private static final String MAX_LENGTH_CONFIGURATION_KEY =
            "${ea.serialization.string.maxLength:" + DEFAULT_MAX_LENGTH + "}";

    private final ConcurrentMap<Class, JacksonMessageDeserializer> deserializers =
            new ConcurrentHashMap<>();
    private final JacksonMessageSerializer serializer;
    private final MessageToStringSerializer toStringSerializer;
    private final ObjectMapper objectMapper;
    private final JacksonActorStateSerializer actorStateSerializer;
    private final JacksonActorStateDeserializer actorStateDeserializer;

    public JacksonSerializationFramework(ObjectMapper objectMapper) {
        this(objectMapper, DEFAULT_MAX_LENGTH, false);
    }

    @Inject
    public JacksonSerializationFramework(
            ObjectMapper objectMapper,
            @Value(MAX_LENGTH_CONFIGURATION_KEY) int maxLength,
            @Value("${ea.logging.useToString:false}") boolean useToString) {
        this.objectMapper = objectMapper;
        this.serializer = new JacksonMessageSerializer(objectMapper);
        this.toStringSerializer = useToString
                ? new PlainMessageToStringSerializer(maxLength)
                : new JacksonMessageToStringSerializer(objectMapper, maxLength);
        this.actorStateSerializer = new JacksonActorStateSerializer(objectMapper);
        this.actorStateDeserializer = new JacksonActorStateDeserializer(objectMapper);
    }

    @Override
    public void register(Class<?> messageClass) {
        Message messageAnnotation;
        if((messageAnnotation = messageClass.getAnnotation(Message.class)) != null
           && this.getClass().equals(messageAnnotation.serializationFramework()))  {
            deserializers.putIfAbsent(messageClass,new JacksonMessageDeserializer(messageClass,objectMapper));
        }
    }

    @Override
    public <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        return serializer;
    }

    @Override
    public <T> MessageToStringSerializer<T> getToStringSerializer(Class<T> messageClass) {
        return toStringSerializer;
    }

    @Override
    public <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass) {
        return deserializers.get(messageClass);
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
}
