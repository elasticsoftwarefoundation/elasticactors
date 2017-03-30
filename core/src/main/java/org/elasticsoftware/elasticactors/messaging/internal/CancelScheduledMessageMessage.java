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

package org.elasticsoftware.elasticactors.messaging.internal;

import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.SystemSerializationFramework;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
@Message(immutable = true, durable = false, serializationFramework = SystemSerializationFramework.class)
public class CancelScheduledMessageMessage implements Serializable {
    private final UUID messageId;
    private final long fireTime;

    public CancelScheduledMessageMessage(UUID messageId, long fireTime) {
        this.messageId = messageId;
        this.fireTime = fireTime;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public long getFireTime() {
        return fireTime;
    }
}
