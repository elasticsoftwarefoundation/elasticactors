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

/**
 * A serializer that converts a message to a String. Its primary usage is logging messages that
 * cause unexpected exceptions.
 *
 * <br><br>
 * Implementations are required to:
 * <ol>
 *     <li>Use the {@link Object#toString()} method when {@value #LOGGING_USE_TO_STRING_PROPERTY}
 *     property is set to {@code true},</li>
 *     <li>Obey the maximum length limit set using the {@value #LOGGING_MAXIMUM_LENGTH_PROPERTY}
 *     property (default: {@value #DEFAULT_MAX_LENGTH}), and</li>
 *     <li>Prefix overflowing content with {@value #CONTENT_TOO_BIG_PREFIX}.</li>
 * </ol>
 *
 * <br><br>
 * <strong>IMPORTANT:</strong> due to the fact messages can potentially contain sensitive data,
 * implementations should be careful not to expose such data in the converted message body.
 */
public interface MessageToStringSerializer<T> {

    String LOGGING_USE_TO_STRING_PROPERTY = "ea.logging.message.useToString";
    String LOGGING_MAXIMUM_LENGTH_PROPERTY = "ea.logging.message.maxLength";
    String CONTENT_TOO_BIG_PREFIX = "[CONTENT_TOO_BIG]: ";
    int DEFAULT_MAX_LENGTH = 5_000;

    /**
     * Serializes a message into a String
     *
     * @param message the message object
     * @return the message serialized into a String
     * @throws Exception if something unexpected happens
     */
    String serialize(T message) throws Exception;
}
