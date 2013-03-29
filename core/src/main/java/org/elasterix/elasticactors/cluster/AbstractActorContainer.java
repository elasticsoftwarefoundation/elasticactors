/*
 * Copyright (c) 2013 Joost van de Wijgerd <jwijgerd@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasterix.elasticactors.cluster;

import org.elasterix.elasticactors.ActorContainer;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.PhysicalNode;
import org.elasterix.elasticactors.messaging.InternalMessage;
import org.elasterix.elasticactors.messaging.MessageHandler;
import org.elasterix.elasticactors.messaging.MessageQueue;
import org.elasterix.elasticactors.messaging.MessageQueueFactory;

/**
 * @author Joost van de Wijgerd
 */
public abstract class AbstractActorContainer implements ActorContainer,MessageHandler {
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
}
