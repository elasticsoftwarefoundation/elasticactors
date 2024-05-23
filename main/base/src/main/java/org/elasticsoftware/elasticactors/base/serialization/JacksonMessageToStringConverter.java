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
import org.elasticsoftware.elasticactors.serialization.MessageToStringConverter;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class JacksonMessageToStringConverter implements MessageToStringConverter {

    private final ObjectMapper objectMapper;
    private final int maxLength;
    private final boolean useToStringForTransient;

    public JacksonMessageToStringConverter(
        ObjectMapper objectMapper,
        int maxLength,
        boolean useToStringForTransient)
    {
        this.objectMapper = objectMapper;
        this.maxLength = maxLength;
        this.useToStringForTransient = useToStringForTransient;
    }

    @Override
    @Nonnull
    public String convert(@Nonnull ByteBuffer message) throws Exception {
        int position = message.position();
        try {
            CharSequence s = StandardCharsets.UTF_8.decode(message);
            return trim(s);
        } finally {
            message.position(position);
        }
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
