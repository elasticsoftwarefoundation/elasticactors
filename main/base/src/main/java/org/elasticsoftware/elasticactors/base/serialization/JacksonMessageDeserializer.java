/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
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
        if (serializedObject.hasArray()) {
            return objectMapper.readValue(serializedObject.array(), objectClass);
        } else {
            // Avoid marking the buffer (what if it was marked from the outside?)
            int position = serializedObject.position();
            try {
                return objectMapper.readValue(
                    new ByteBufferBackedInputStream(serializedObject),
                    objectClass
                );
            } finally {
                serializedObject.position(position);
            }
        }
    }

    @Override
    public Class<T> getMessageClass() {
        return objectClass;
    }

    @Override
    public boolean isSafe() {
        return true;
    }
}
