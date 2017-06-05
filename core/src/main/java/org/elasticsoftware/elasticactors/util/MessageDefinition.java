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

import org.elasticsoftware.elasticactors.serialization.Message;

/**
 * @author Joost van de Wijgerd
 */
public final class MessageDefinition {
    private final Class messageClass;
    private final String messageType;
    private final String messageVersion;
    private final boolean durable;
    private final boolean immutable;
    private final int timeout;

    MessageDefinition(Class<?> messageClass) {
        this.messageClass = messageClass;
        Message messageAnnotation = messageClass.getAnnotation(Message.class);
        durable = (messageAnnotation != null) && messageAnnotation.durable();
        immutable = (messageAnnotation != null) && messageAnnotation.immutable();
        timeout = (messageAnnotation != null) ? messageAnnotation.timeout() : Message.NO_TIMEOUT;
        messageType = (messageAnnotation != null)
                ? (messageAnnotation.type().equals(Message.DEFAULT_TYPE) ? messageClass.getName() : messageAnnotation.type())
                : messageClass.getName();
        messageVersion = (messageAnnotation != null) ? messageAnnotation.version() : Message.DEFAULT_VERSION;
    }

    public Class getMessageClass() {
        return messageClass;
    }

    public String getMessageType() {
        return messageType;
    }

    public String getMessageVersion() {
        return messageVersion;
    }

    public boolean isDurable() {
        return durable;
    }

    public boolean isImmutable() {
        return immutable;
    }

    public int getTimeout() {
        return timeout;
    }
}
