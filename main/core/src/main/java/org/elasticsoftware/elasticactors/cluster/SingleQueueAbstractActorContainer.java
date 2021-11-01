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

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;

/**
 * @author Joost van de Wijgerd
 */
public abstract class SingleQueueAbstractActorContainer extends AbstractActorContainer {

    private MessageQueue messageQueue;

    public SingleQueueAbstractActorContainer(MessageQueueFactory messageQueueFactory, ActorRef myRef, PhysicalNode node) {
        super(myRef, messageQueueFactory, node);
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
    public final void offerInternalMessage(InternalMessage message) {
        messageQueue.add(message);
    }
}
