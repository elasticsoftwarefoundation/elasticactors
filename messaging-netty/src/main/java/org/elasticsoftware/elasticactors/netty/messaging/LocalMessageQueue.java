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

package org.elasticsoftware.elasticactors.netty.messaging;

import org.elasticsoftware.elasticactors.messaging.*;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class LocalMessageQueue extends PersistentMessageQueue implements MessageHandlerEventListener {
    private final LinkedBlockingQueue<InternalMessage> queue;
    private final MessageQueueEventListener eventListener;
    private final MessageHandler messageHandler;
    private final InternalMessageDeserializer internalMessageDeserializer;

    protected LocalMessageQueue(String name, MessageQueueEventListener eventListener, MessageHandler messageHandler, InternalMessageDeserializer internalMessageDeserializer) {
        this(name, Integer.MAX_VALUE, eventListener, messageHandler,internalMessageDeserializer);
    }

    protected LocalMessageQueue(String name, int capacity, MessageQueueEventListener eventListener, MessageHandler messageHandler, InternalMessageDeserializer internalMessageDeserializer) {
        super(name);
        this.eventListener = eventListener;
        this.messageHandler = messageHandler;
        queue = new LinkedBlockingQueue<>(capacity);
        this.internalMessageDeserializer = internalMessageDeserializer;
    }

    @Override
    public void initialize() throws Exception {
        List<CommitLog.CommitLogEntry> pendingEntries = commitLog.replay(getName());
        for (CommitLog.CommitLogEntry pendingEntry : pendingEntries) {
            queue.offer(internalMessageDeserializer.deserialize(pendingEntry.getData()));
        }
        // wake up the listener
        eventListener.wakeUp();
    }

    @Override
    public void destroy() {
        eventListener.onDestroy(this);
    }

    @Override
    protected void doOffer(InternalMessage message) {
        queue.offer(message);
        eventListener.wakeUp();
    }

    public InternalMessage peek() {
        return queue.peek();
    }


    public MessageHandler getMessageHandler() {
        return messageHandler;
    }

    @Override
    public boolean add(InternalMessage message) {
        try {
            return queue.add(message);
        } finally {
            eventListener.wakeUp();
        }
    }

    @Override
    public InternalMessage poll() {
        return queue.poll();
    }

    public MessageHandlerEventListener getMessageHandlerEventListener() {
        return this;
    }

    @Override
    public void onError(InternalMessage message, Throwable exception) {
        logger.error(format("Exception when handling message of type [%s] for Actor [%s]", message.getPayloadClass(),
                                                                                           message.getReceiver().toString()));
        //@todo: determine what to do, for now just ack anyway
        ack(message);
    }

    @Override
    public void onDone(InternalMessage message) {
        ack(message);
    }
}
