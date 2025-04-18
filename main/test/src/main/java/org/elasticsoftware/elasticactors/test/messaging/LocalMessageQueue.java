/*
 * Copyright 2013 - 2025 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.test.messaging;

import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Joost van de Wijgerd
 */
public final class LocalMessageQueue implements MessageQueue {

    private final static Logger logger = LoggerFactory.getLogger(LocalMessageQueue.class);

    private final String queueName;
    private final MessageHandler messageHandler;
    private final TransientAck transientAck = new TransientAck();
    private final ThreadBoundExecutor queueExecutor;

    private static final List<Throwable> thrownExceptions = new CopyOnWriteArrayList<>();

    public static List<Throwable> getThrownExceptions() {
        return ImmutableList.copyOf(thrownExceptions);
    }

    public LocalMessageQueue(ThreadBoundExecutor queueExecutor,String queueName, MessageHandler messageHandler) {
        this.queueExecutor = queueExecutor;
        this.queueName = queueName;
        this.messageHandler = messageHandler;
    }

    @Override
    public boolean offer(final InternalMessage message) {
        // execute on a seperate (thread bound) executor
        queueExecutor.execute(new InternalMessageHandler(queueName,message,messageHandler,transientAck,logger));
        return true;

    }

    @Override
    public boolean add(InternalMessage message) {
        return offer(message);
    }

    @Override
    public InternalMessage poll() {
        return null;
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public void initialize() throws Exception {
        logger.info("Starting (test) local message queue [{}]", queueName);
    }

    @Override
    public void destroy() {
        logger.info("Stopping (test) local message queue [{}]", queueName);
        thrownExceptions.clear();
    }


    private static final class InternalMessageHandler implements ThreadBoundRunnable<String> {
        private final String queueName;
        private final InternalMessage message;
        private final MessageHandler messageHandler;
        private final MessageHandlerEventListener listener;
        private final Logger logger;

        private InternalMessageHandler(String queueName, InternalMessage message, MessageHandler messageHandler, MessageHandlerEventListener listener, Logger logger) {
            this.queueName = queueName;
            this.message = message;
            this.messageHandler = messageHandler;
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public String getKey() {
            return queueName;
        }

        @Override
        public void run() {
            try {
                messageHandler.handleMessage(message,listener);
            } catch(Exception e) {
                logger.error("Unexpected exception on #handleMessage",e);
            }
        }
    }

    private final class TransientAck implements MessageHandlerEventListener {

        @Override
        public void onError(InternalMessage message, Throwable exception) {
            logger.error("Error handling transient message, payloadClass [{}]",message.getPayloadClass(),exception);
            thrownExceptions.add(exception);
        }

        @Override
        public void onDone(InternalMessage message) {
            // do nothing
        }
    }
}
