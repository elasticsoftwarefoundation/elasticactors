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
import org.elasterix.elasticactors.serialization.protobuf.Elasticactors;

/**
 * @author Joost van de Wijgerd
 */
public final class RemoteMessageQueue extends PersistentMessageQueue {
    private final MessagingService messagingService;
    private final MessageHandler messageHandler;

    public RemoteMessageQueue(String name, MessagingService messagingService, MessageHandler messageHandler) {
        super(name);
        this.messagingService = messagingService;
        this.messageHandler = messageHandler;
    }

    @Override
    public void initialize() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void destroy() {

    }

    @Override
    protected void doOffer(InternalMessage message, byte[] serializedMessage) {
        // @todo: send message to remote server via messaging service
        Elasticactors.WireMessage.Builder builder = Elasticactors.WireMessage.newBuilder();
        builder.setQueueName(getName());
        builder.setInternalMessage(ByteString.copyFrom(serializedMessage));
        messagingService.sendWireMessage(builder.build(),messageHandler.getPhysicalNode());
    }

    @Override
    public boolean add(InternalMessage message) {
        return offer(message);
    }

    /**
     * @return this will always return null as a {@link RemoteMessageQueue} cannot be polled locally
     */
    @Override
    public InternalMessage poll() {
        return null;
    }
}
