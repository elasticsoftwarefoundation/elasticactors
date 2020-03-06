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
import org.elasticsoftware.elasticactors.serialization.MessageToStringSerializer;

public final class JacksonMessageToStringSerializer<T> implements MessageToStringSerializer<T> {

    private final ObjectMapper objectMapper;
    private final int maxLength;

    public JacksonMessageToStringSerializer(ObjectMapper objectMapper, int maxLength) {
        this.objectMapper = objectMapper;
        this.maxLength = maxLength;
    }

    @Override
    public String serialize(T message) throws Exception {
        String serialized = objectMapper.writeValueAsString(message);
        if (serialized != null && serialized.length() > maxLength) {
            serialized = "[CONTENT_TOO_BIG]: " + serialized.substring(0, maxLength);
        }
        return serialized;
    }
}
