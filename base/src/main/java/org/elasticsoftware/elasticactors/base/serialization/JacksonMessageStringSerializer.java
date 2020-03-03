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
import org.elasticsoftware.elasticactors.serialization.MessageStringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public final class JacksonMessageStringSerializer<T> implements MessageStringSerializer<T> {

    private static final Logger logger =
            LoggerFactory.getLogger(JacksonMessageStringSerializer.class);

    private final ObjectMapper objectMapper;

    public JacksonMessageStringSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Nullable
    @Override
    public String serialize(@Nullable T message) {
        try {
            if (message != null) {
                return objectMapper.writeValueAsString(message);
            }
        } catch (Exception e) {
            logger.error(
                    "Exception thrown while converting message of type [{}] to String",
                    message.getClass().getName(),
                    e);
        }
        return null;
    }
}
