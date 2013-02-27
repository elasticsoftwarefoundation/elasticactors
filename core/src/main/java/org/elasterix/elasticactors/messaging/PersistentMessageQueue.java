/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.messaging;

import com.google.protobuf.ByteString;
import org.apache.log4j.Logger;
import org.elasterix.elasticactors.serialization.internal.ActorRefSerializer;
import org.elasterix.elasticactors.serialization.protobuf.Elasticactors.InternalMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public abstract class PersistentMessageQueue implements MessageQueue {
    private static final Logger logger = Logger.getLogger(PersistentMessageQueue.class);
    private final ActorRefSerializer actorRefSerializer = ActorRefSerializer.get();
    private final String name;
    private CommitLog commitLog;

    protected PersistentMessageQueue(String name) {
        this.name = name;
    }

    @Override
    public boolean offer(org.elasterix.elasticactors.messaging.InternalMessage message) {
        InternalMessage.Builder builder = InternalMessage.newBuilder();
        builder.setPayload(ByteString.copyFrom(message.getPayload()));
        builder.setPayloadClass(message.getPayloadClass().getName());
        builder.setReceiver(actorRefSerializer.serialize(message.getReceiver()));
        if(message.getSender() != null) {
            builder.setSender(actorRefSerializer.serialize(message.getSender()));
        }
        byte[] messageData = builder.build().toByteArray();
        try {
            commitLog.append(name, message.getId(), messageData);
            doOffer(message,messageData);
        } catch(Exception e) {
            logger.error("Exception on offer",e);
            return false;
        }
        return true;
    }

    @Override
    public String getName() {
        return name;
    }

    protected abstract void doOffer(org.elasterix.elasticactors.messaging.InternalMessage message,byte[] serializedMessage);

    @Autowired
    public void setCommitLog(CommitLog commitLog) {
        this.commitLog = commitLog;
    }
}
