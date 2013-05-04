/*
 * Copyright 2013 Joost van de Wijgerd
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

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public final class JacksonMessageDeserializer<T> implements MessageDeserializer<T> {
    //private final TypeReference<T> typeReference = new TypeReference<T>() {};
    private final ObjectMapper objectMapper;
    private final Class<T> objectClass;

    public JacksonMessageDeserializer(Class<T> objectClass,ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.objectClass = objectClass;
    }

    @Override
    public T deserialize(ByteBuffer serializedObject) throws IOException {
        byte[] buf = new byte[serializedObject.remaining()];
        serializedObject.get(buf);
        return objectMapper.readValue(buf, objectClass);
    }
}
