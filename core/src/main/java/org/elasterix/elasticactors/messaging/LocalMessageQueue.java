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

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Joost van de Wijgerd
 */
public class LocalMessageQueue extends PersistentMessageQueue {
    private final LinkedBlockingQueue<InternalMessage> queue;
    private final MessageQueueEventListener eventListener;
    private final MessageHandler messageHandler;

    protected LocalMessageQueue(String name, MessageQueueEventListener eventListener, MessageHandler messageHandler) {
        this(name, Integer.MAX_VALUE, eventListener, messageHandler);
    }

    protected LocalMessageQueue(String name, int capacity, MessageQueueEventListener eventListener, MessageHandler messageHandler) {
        super(name);
        this.eventListener = eventListener;
        this.messageHandler = messageHandler;
        queue = new LinkedBlockingQueue<InternalMessage>(capacity);
    }

    @Override
    public void destroy() {
        eventListener.onDestroy(this);
    }

    @Override
    protected void doOffer(InternalMessage message, byte[] serializedMessage) {
        queue.offer(message);
        eventListener.signal();
    }

    protected InternalMessage peek() {
        return queue.peek();
    }

    protected MessageHandler getMessageHandler() {
        return messageHandler;
    }

    @Override
    public boolean add(InternalMessage message) {
        try {
            return queue.add(message);
        } finally {
            eventListener.signal();
        }
    }

    @Override
    public InternalMessage poll() {
        return queue.poll();
    }
}
