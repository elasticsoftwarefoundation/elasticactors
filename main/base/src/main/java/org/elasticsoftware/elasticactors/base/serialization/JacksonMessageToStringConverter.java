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
import org.elasticsoftware.elasticactors.serialization.MessageToStringConverter;
import org.springframework.core.env.Environment;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class JacksonMessageToStringConverter implements MessageToStringConverter {

    private final ObjectMapper objectMapper;
    private final int maxLength;
    private final boolean useToStringForTransient;

    public JacksonMessageToStringConverter(ObjectMapper objectMapper, Environment environment) {
        this.objectMapper = objectMapper;
        this.maxLength = environment.getProperty(
            LOGGING_MAXIMUM_LENGTH_PROPERTY,
            Integer.class,
            DEFAULT_MAX_LENGTH
        );
        this.useToStringForTransient =
            environment.getProperty(LOGGING_USE_TO_STRING_PROPERTY, Boolean.class, false);
    }

    @Override
    @Nonnull
    public String convert(@Nonnull ByteBuffer message) throws Exception {
        CharSequence s = StandardCharsets.UTF_8.decode(message);
        return trim(s);
    }

    @Override
    @Nonnull
    public String convert(@Nonnull Object message) throws Exception {
        String s = useToStringForTransient
            ? message.toString()
            : objectMapper.writeValueAsString(message);
        return trim(s);
    }

    private String trim(CharSequence s) {
        if (s.length() > maxLength) {
            return new StringBuilder(CONTENT_TOO_BIG_PREFIX.length() + maxLength)
                .append(CONTENT_TOO_BIG_PREFIX)
                .append(s, 0, maxLength)
                .toString();
        } else {
            return s.toString();
        }
    }
}
