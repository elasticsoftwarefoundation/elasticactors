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

package org.elasticsoftware.elasticactors.messaging;

import org.elasticsoftware.elasticactors.ActorRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SingleMessageQueueProxy implements MessageQueueProxy {

    private final static Logger logger = LoggerFactory.getLogger(SingleMessageQueueProxy.class);

    private final MessageQueueFactory messageQueueFactory;
    private final MessageHandler messageHandler;
    private final ActorRef actorRef;

    private MessageQueue messageQueue;

    public SingleMessageQueueProxy(
        MessageQueueFactory messageQueueFactory,
        MessageHandler messageHandler,
        ActorRef actorRef)
    {
        this.messageQueueFactory = messageQueueFactory;
        this.messageHandler = messageHandler;
        this.actorRef = actorRef;
    }

    @Override
    public synchronized void init() throws Exception {
        logger.info(
            "Starting up queue proxy for [{}] in Single-Queue mode",
            actorRef.getActorPath()
        );
        this.messageQueue = messageQueueFactory.create(actorRef.getActorPath(), messageHandler);
    }

    @Override
    public void destroy() {
        // release all resources
        this.messageQueue.destroy();
    }

    @Override
    public void offerInternalMessage(InternalMessage message) {
        messageQueue.add(message);
    }
}
