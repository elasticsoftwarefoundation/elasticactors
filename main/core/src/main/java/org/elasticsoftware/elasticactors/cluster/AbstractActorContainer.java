/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.cluster;

import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

/**
 * @author Joost van de Wijgerd
 */
public abstract class AbstractActorContainer implements ActorContainer, MessageHandler {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
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
        try (MessagingScope ignored = getManager().enter(internalMessage)) {
            if (senderRef instanceof ActorContainerRef && !internalMessage.isUndeliverable()) {
                ((ActorContainerRef) senderRef).getActorContainer().undeliverableMessage(internalMessage, receiverRef);
            } else if (internalMessage.isUndeliverable()) {
                logger.error(
                        "Receiver for undeliverable message not found. "
                                + "Message type [{}]. "
                                + "Receiver [{}]. "
                                + "Sender [{}].",
                        internalMessage.getPayloadClass(),
                        receiverRef,
                        senderRef);
            } else {
                logger.warn(
                        "Could not send message undeliverable. "
                                + "Original message type [{}]. "
                                + "Receiver [{}]. "
                                + "Sender [{}].",
                        internalMessage.getPayloadClass(),
                        receiverRef,
                        senderRef);
            }
        } finally {
            // ack anyway
            messageHandlerEventListener.onDone(internalMessage);
        }
    }

    @Override
    public void sendMessage(ActorRef from, ActorRef to, Object message) throws Exception {
        sendMessage(from, ImmutableList.of(to), message);
    }
}
