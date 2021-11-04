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

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

/**
 * A serializer that converts a message's serialized payload to a String.
 * Its primary usage is logging messages that cause unexpected exceptions.
 */
public interface MessageToStringConverter {

    String LOGGING_USE_TO_STRING_PROPERTY = "ea.logging.messages.transient.useToString";
    String LOGGING_MAXIMUM_LENGTH_PROPERTY = "ea.logging.messages.maxLength";
    String CONTENT_TOO_BIG_PREFIX = "[CONTENT_TOO_BIG]: ";
    int DEFAULT_MAX_LENGTH = 5_000;

    /**
     * Converts a message's payload into a String
     *
     * @param byteBuffer the message's internal byte buffer
     * @return the message converted into a String
     */
    @Nonnull
    String convert(@Nonnull ByteBuffer byteBuffer) throws Exception;

    /**
     * Converts a transient message into a String.
     * This might incur a performance penalty if the message has to be serialized to be logged.
     *
     * @param message the message's payload object
     * @return the message converted into a String
     */
    @Nonnull
    String convert(@Nonnull Object message) throws Exception;
}
