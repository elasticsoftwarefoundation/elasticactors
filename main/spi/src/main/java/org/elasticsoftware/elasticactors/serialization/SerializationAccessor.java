/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.serialization;

public interface SerializationAccessor {

    /**
     * Return the serializer for the given message type
     *
     * @param messageClass
     * @param <T>
     * @return
     */
    <T> MessageSerializer<T> getSerializer(Class<T> messageClass);

    /**
     * Return the deserializer for the give message type
     *
     * @param messageClass
     * @param <T>
     * @return
     */
    <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass);
}
