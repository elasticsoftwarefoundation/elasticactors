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

package org.elasticsoftware.elasticactors.messaging.reactivestreams;

import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.SystemSerializationFramework;

/**
 * @author Joost van de Wijgerd
 */
@Message(immutable = true, durable = false, serializationFramework = SystemSerializationFramework.class)
public final class NextMessage implements ReactiveStreamsProtocol {
    private final String messageType;
    private final byte[] messageBytes;
    private final String messageVersion;

    public NextMessage(String messageType, String messageVersion, byte[] messageBytes) {
        this.messageType = messageType;
        this.messageBytes = messageBytes;
        this.messageVersion = messageVersion;
    }

    public String getMessageType() {
        return messageType;
    }

    public String getMessageVersion() {
        return messageVersion;
    }

    public byte[] getMessageBytes() {
        return messageBytes;
    }
}
