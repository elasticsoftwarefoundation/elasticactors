/*
 * Copyright 2013 the original authors
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

package org.elasticsoftware.elasticactors.netty.messaging;

import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessagingService;

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
    protected void doOffer(InternalMessage message) {
        messagingService.sendWireMessage(getName(),message.toByteArray(),messageHandler.getPhysicalNode());
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
