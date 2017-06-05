/*
 * Copyright 2013 - 2017 The Original Authors
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

package org.elasticsoftware.elasticactors.util;

import javax.annotation.Nullable;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMapWithExpectedSize;

/**
 * @author Joost van de Wijgerd
 */
public final class MessageTools {
    private static Map<Class<?>, MessageDefinition> cacheByClass = newHashMapWithExpectedSize(1024);
    private static Map<TypeVersionPair, MessageDefinition> cacheByType = newHashMapWithExpectedSize(1024);

    private MessageTools() {}

    /**
     * This method should only be called while initializing!
     *
     * @param messageClass
     */
    public static void register(Class<?> messageClass) {
        MessageDefinition messageDefinition = new MessageDefinition(messageClass);
        cacheByClass.put(messageClass, messageDefinition);
        cacheByType.put(new TypeVersionPair(messageDefinition.getMessageType(), messageDefinition.getMessageType()), messageDefinition);
    }

    public static MessageDefinition getMessageDefinition(Class<?> messageClass) {
        MessageDefinition messageDefinition = cacheByClass.get(messageClass);
        return messageDefinition != null ? messageDefinition : new MessageDefinition(messageClass);
    }

    public static @Nullable MessageDefinition getMessageDefinition(String messageType, String messageVersion) {
        return cacheByType.get(new TypeVersionPair(messageType, messageVersion));
    }
}
