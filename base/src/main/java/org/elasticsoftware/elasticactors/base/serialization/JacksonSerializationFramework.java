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

package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

import javax.inject.Named;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Joost van de Wijgerd
 */
@Named
public class JacksonSerializationFramework implements SerializationFramework {
    private final ConcurrentMap<Class,JacksonMessageDeserializer> deserializers = new ConcurrentHashMap<>();
    private final JacksonMessageSerializer serializer;
    private final ObjectMapper objectMapper;

    public JacksonSerializationFramework(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.serializer = new JacksonMessageSerializer(objectMapper);
    }

    public void register(Class<?> messageClass) {
        Message messageAnnotation;
        if((messageAnnotation = messageClass.getAnnotation(Message.class)) != null
           && this.getClass().equals(messageAnnotation.serializationFramework()))  {
            deserializers.putIfAbsent(messageClass,new JacksonMessageDeserializer(messageClass,objectMapper));
        }
    }
}
