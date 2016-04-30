/*
 * Copyright 2013 - 2016 The Original Authors
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

package org.elasticsoftware.elasticactors.activemq;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.messaging.*;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static org.apache.activemq.artemis.api.core.Message.HDR_DUPLICATE_DETECTION_ID;
import static org.elasticsoftware.elasticactors.messaging.UUIDTools.toByteArray;

/**
 * @author Joost van de Wijgerd
 */
public final class LocalMessageQueue implements MessageQueue, org.apache.activemq.artemis.api.core.client.MessageHandler {
    private static final Logger logger = LogManager.getLogger(LocalMessageQueue.class);

    private final ThreadBoundExecutor queueExecutor;
    private final InternalMessageDeserializer internalMessageDeserializer;
    private final String queueName;
    private final String routingKey;
    private final ClientSession clientSession;
    private final ClientProducer producer;
    private final ClientConsumer consumer;
    private final MessageHandler messageHandler;
    private final CountDownLatch destroyLatch = new CountDownLatch(1);
    private final AtomicBoolean recovering = new AtomicBoolean(false);
    private final TransientAck transientAck = new TransientAck();

    LocalMessageQueue(ThreadBoundExecutor queueExecutor, InternalMessageDeserializer internalMessageDeserializer,
                             String queueName, String routingKey, ClientSession clientSession,
                             ClientProducer producer,  MessageHandler messageHandler) throws ActiveMQException {
        this.queueExecutor = queueExecutor;
        this.internalMessageDeserializer = internalMessageDeserializer;
        this.queueName = queueName;
        this.routingKey = routingKey;
        this.clientSession = clientSession;
        this.producer = producer;
        this.consumer = clientSession.createConsumer(queueName);
        this.messageHandler = messageHandler;
    }

    @Override
    public boolean offer(InternalMessage message) {
        // see if we are recovering first
        if(this.recovering.get()) {
            throw new MessageDeliveryException("MessagingService is recovering",true);
        }
        if(!message.isDurable()) {
            // execute on a separate (thread bound) executor
            queueExecutor.execute(new InternalMessageHandler(queueName,message,messageHandler,transientAck,logger));
            return true;
        } else {
            ClientMessage clientMessage = clientSession.createMessage(message.isDurable());
            clientMessage.getBodyBuffer().writeBytes(message.toByteArray());
            clientMessage.putStringProperty("routingKey", routingKey);
            // use the duplicate detection from
            clientMessage.putBytesProperty(HDR_DUPLICATE_DETECTION_ID, toByteArray(message.getId()));
            try {
                producer.send(clientMessage);
                return true;
            } catch (ActiveMQException e) {
                throw new MessageDeliveryException("IOException while publishing message",e,false);
            } /*catch(SomeRecoverableException e) { @todo: figure out which exceptions are recoverable
                this.recovering.set(true);
                throw new MessageDeliveryException("MessagingService is recovering",true);
            } */
        }
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
        consumer.setMessageHandler(this);
    }

    @Override
    public void destroy() {
        try {
            consumer.close();
            destroyLatch.await(4, TimeUnit.SECONDS);
        } catch (ActiveMQException | InterruptedException e) {
            logger.warn("Exception while closing consumer", e);
        }
    }

    @Override
    public void onMessage(ClientMessage message) {
        byte[] bodyBuffer = new byte[message.getBodySize()];
        message.getBodyBuffer().readBytes(bodyBuffer);
        // execute on separate (thread bound) executor
        queueExecutor.execute(new ActiveMQMessageHandler(queueName,bodyBuffer,internalMessageDeserializer,messageHandler,new ActiveMQAck(message),logger));
    }

    private static final class ActiveMQMessageHandler implements ThreadBoundRunnable<String> {
        private final String queueName;
        private final InternalMessageDeserializer internalMessageDeserializer;
        private final byte[] body;
        private final org.elasticsoftware.elasticactors.messaging.MessageHandler messageHandler;
        private final MessageHandlerEventListener listener;
        private final Logger logger;

        private ActiveMQMessageHandler(String queueName, byte[] body, InternalMessageDeserializer internalMessageDeserializer,
                                       org.elasticsoftware.elasticactors.messaging.MessageHandler messageHandler,
                                       MessageHandlerEventListener listener, Logger logger) {
            this.queueName = queueName;
            this.internalMessageDeserializer = internalMessageDeserializer;
            this.body = body;
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
            InternalMessage message = null;
            try {
                // get the body data
                message = internalMessageDeserializer.deserialize(body);
                messageHandler.handleMessage(message,listener);
            } catch(Exception e) {
                logger.error("Unexpected exception on #handleMessage",e);
            } finally {
                // @todo: performance logging here
            }
        }
    }

    private final class ActiveMQAck implements MessageHandlerEventListener {
        private final ClientMessage clientMessage;
        //private final InternalMessage message;

        private ActiveMQAck(ClientMessage clientMessage) {
            this.clientMessage = clientMessage;
        }

        @Override
        public void onError(final InternalMessage message,final Throwable exception) {
            onDone(message);
        }

        @Override
        public void onDone(final InternalMessage message) {
            try {
                this.clientMessage.acknowledge();
            } catch (ActiveMQException e) {
                logger.error("Exception while acking message", e);
            }
        }
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
            } finally {
                // @todo: add performance logging here
            }
        }
    }

    private final class TransientAck implements MessageHandlerEventListener {

        @Override
        public void onError(InternalMessage message, Throwable exception) {
            logger.error(format("Error handling transient message, payloadClass [%s]", message.getPayloadClass()),exception);
        }

        @Override
        public void onDone(InternalMessage message) {
            // do nothing
        }
    }

}
