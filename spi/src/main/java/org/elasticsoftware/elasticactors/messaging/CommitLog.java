/*
 * Copyright 2013 - 2014 The Original Authors
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

package org.elasticsoftware.elasticactors.messaging;

import java.util.List;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public interface CommitLog {
    /**
     * Append a message to the commit log for the give segment (queue / actor system shard)
     *
     * @param segment           the name of the shard
     * @param messageId         a time based UUID of the message {@link org.elasticsoftware.elasticactors.messaging.UUIDTools#createTimeBasedUUID()}
     * @param data              the serialized payload of the message
     */
    void append(String segment, UUID messageId, byte[] data);

    /**
     * Delete a message from the commit log. Meant to be called after message was successfully added
     *
     * @param segment           the name of the shard
     * @param messageId         message id to delete
     */
    void delete(String segment, UUID messageId);

    /**
     * Return the non-deleted messages for this shard. To be used when starting up a queue
     *
     * @todo: this should probably be implemented with the Visitor pattern
     *
     * @param segment
     * @return
     */
    List<CommitLogEntry> replay(String segment);

    /**
     * Representation of and entry in the {@link CommitLog}
     */
    public class CommitLogEntry {
        private final UUID messageId;
        private final byte[] data;

        public CommitLogEntry(UUID messageId, byte[] data) {
            this.messageId = messageId;
            this.data = data;
        }

        public UUID getMessageId() {
            return messageId;
        }

        public byte[] getData() {
            return data;
        }
    }
}
