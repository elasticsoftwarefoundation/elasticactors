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

package org.elasticsoftware.elasticactors.cluster;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.messaging.*;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public abstract class AbstractActorContainer implements ActorContainer, MessageHandler {
    private final Logger logger = LogManager.getLogger(getClass());
    private final ActorRef myRef;
    private final MessageQueueFactory messageQueueFactory;
    protected MessageQueue messageQueue;
    private final PhysicalNode localNode;

    public AbstractActorContainer(MessageQueueFactory messageQueueFactory, ActorRef myRef, PhysicalNode node) {
        this.messageQueueFactory = messageQueueFactory;
        this.myRef = myRef;
        this.localNode = node;
    }

    @Override
    public void init() throws Exception {
        this.messageQueue = messageQueueFactory.create(myRef.getActorPath(), this);
    }

    @Override
    public void destroy() {
        // release all resources
        this.messageQueue.destroy();
    }

    @Override
    public final ActorRef getActorRef() {
        return myRef;
    }

    @Override
    public final void offerInternalMessage(InternalMessage message) {
        messageQueue.add(message);
    }

    @Override
    public final PhysicalNode getPhysicalNode() {
        return localNode;
    }

    protected void handleUndeliverable(InternalMessage internalMessage, ActorRef receiverRef, MessageHandlerEventListener messageHandlerEventListener) throws Exception {
        // if a message-undeliverable is undeliverable, don't send an undeliverable message back!
        ActorRef senderRef = internalMessage.getSender();
        try {
            if (senderRef != null && senderRef instanceof ActorContainerRef && !internalMessage.isUndeliverable()) {
                ((ActorContainerRef) senderRef).getActorContainer().undeliverableMessage(internalMessage, receiverRef);
            } else if(internalMessage.isUndeliverable()) {
                logger.error(format("Receiver for undeliverable message not found: message type '%s' , receiver '%s'", internalMessage.getPayloadType(), receiverRef.toString()));
            } else {
                logger.warn("Could not send message undeliverable");
            }
        } finally {
            // ack anyway
            messageHandlerEventListener.onDone(internalMessage);
        }
    }

    public void sendMessage(ActorRef from, ActorRef to, Object message) throws Exception {
        sendMessage(from, ImmutableList.of(to), message);
    }
}
