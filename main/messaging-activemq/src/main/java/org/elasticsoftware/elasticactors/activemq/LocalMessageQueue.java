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

package org.elasticsoftware.elasticactors.activemq;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.activemq.artemis.api.core.Message.HDR_DUPLICATE_DETECTION_ID;
import static org.elasticsoftware.elasticactors.messaging.UUIDTools.toByteArray;

/**
 * @author Joost van de Wijgerd
 */
public final class LocalMessageQueue implements MessageQueue, org.apache.activemq.artemis.api.core.client.MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(LocalMessageQueue.class);

    private final ThreadBoundExecutor queueExecutor;
    private final InternalMessageDeserializer internalMessageDeserializer;
    private final String queueName;
    private final String routingKey;
    private final ClientSession clientSession;
    private final ClientProducer producer;
    private final ClientConsumer consumer;
    private final MessageHandler messageHandler;
    private final AtomicBoolean recovering = new AtomicBoolean(false);
    private final TransientAck transientAck = new TransientAck();
    private final ActiveMQMessageProcessor messageProcessor;
    private final CountDownLatch destroyLatch = new CountDownLatch(1);
    private final boolean useMessageHandler;
    private boolean running = true;

    LocalMessageQueue(ThreadBoundExecutor queueExecutor, InternalMessageDeserializer internalMessageDeserializer,
                      String queueName, String routingKey, ClientSession clientSession, ClientProducer clientProducer,
                      MessageHandler messageHandler,
                      boolean useMessageHandler, boolean useImmediateReceive) throws ActiveMQException {
        this.queueExecutor = queueExecutor;
        this.internalMessageDeserializer = internalMessageDeserializer;
        this.queueName = queueName;
        this.routingKey = routingKey;
        this.clientSession = clientSession;
        this.producer = clientProducer;
        this.useMessageHandler = useMessageHandler;
        this.consumer = clientSession.createConsumer(queueName);
        this.messageHandler = messageHandler;
        this.messageProcessor = new ActiveMQMessageProcessor(queueName, internalMessageDeserializer, messageHandler, useImmediateReceive);
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
            queueExecutor.execute(new SendMessage(message));
            return true;
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
    public synchronized void initialize() throws Exception {
        logger.info("Starting local message queue [{}->{}]", routingKey, queueName);
        if(useMessageHandler) {
            consumer.setMessageHandler(this);
        } else {
            // start the receive loop
            receiveMessage();
        }
        clientSession.start();
    }

    @Override
    public void destroy() {
        try {
            logger.info("Stopping local message queue [{}->{}]", routingKey, queueName);
            queueExecutor.execute(new DestroyQueue(queueName));
            destroyLatch.await(3, TimeUnit.SECONDS);
            consumer.close();
            producer.close();
            clientSession.close();
        } catch (ActiveMQException | InterruptedException e) {
            logger.warn("Exception while closing consumer for queue {}", queueName, e);
        }
    }

    @Override
    public void onMessage(ClientMessage message) {
        // execute on separate (thread bound) executor
        queueExecutor.execute(new ActiveMQMessageHandler(
            queueName,
            message.getBodyBuffer().toByteBuffer(),
            internalMessageDeserializer,
            messageHandler,
            new ActiveMQAck(message),
            logger
        ));
    }

    private void receiveMessage() {
        if(running) {
            queueExecutor.execute(messageProcessor);
        } else {
            destroyLatch.countDown();
        }
    }

    private final class ActiveMQMessageProcessor implements ThreadBoundRunnable<String> {
        private final String queueName;
        private final InternalMessageDeserializer internalMessageDeserializer;
        private final org.elasticsoftware.elasticactors.messaging.MessageHandler messageHandler;
        private final boolean receiveImmediate;

        private ActiveMQMessageProcessor(String queueName, InternalMessageDeserializer internalMessageDeserializer,
                                         MessageHandler messageHandler, boolean receiveImmediate) {
            this.queueName = queueName;
            this.internalMessageDeserializer = internalMessageDeserializer;
            this.messageHandler = messageHandler;
            this.receiveImmediate = receiveImmediate;
        }

        @Override
        public String getKey() {
            return queueName;
        }

        @Override
        public void run() {
            try {
                ClientMessage clientMessage = receiveImmediate ? consumer.receiveImmediate() : consumer.receive(1);
                if(clientMessage != null) {
                    // get the body data
                    InternalMessage message = internalMessageDeserializer.deserialize(
                        clientMessage.getBodyBuffer().toByteBuffer()
                    );
                    messageHandler.handleMessage(message, new ActiveMQAck(clientMessage));
                }
            } catch(ActiveMQException e) {
                logger.error("Unexpected exception on consumer.receive*", e);
            } catch(IOException e) {
                logger.error("Exception deserializing InteralMessage", e);
            } catch(Exception e) {
                logger.error("Unexpected exception in handleMessage", e);
            } finally {
                // @todo: performance logging here
                // we reschedule ourselves for the next run
                receiveMessage();
            }
        }

    }

    private static final class ActiveMQMessageHandler implements ThreadBoundRunnable<String> {
        private final String queueName;
        private final InternalMessageDeserializer internalMessageDeserializer;
        private final ByteBuffer body;
        private final org.elasticsoftware.elasticactors.messaging.MessageHandler messageHandler;
        private final MessageHandlerEventListener listener;
        private final Logger logger;

        private ActiveMQMessageHandler(
            String queueName,
            ByteBuffer body,
            InternalMessageDeserializer internalMessageDeserializer,
            org.elasticsoftware.elasticactors.messaging.MessageHandler messageHandler,
            MessageHandlerEventListener listener,
            Logger logger)
        {
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
            InternalMessage message;
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

        private ActiveMQAck(ClientMessage clientMessage) {
            this.clientMessage = clientMessage;
        }

        @Override
        public void onError(final InternalMessage message,final Throwable exception) {
            onDone(message);
        }

        @Override
        public void onDone(final InternalMessage message) {
            queueExecutor.execute(new AcknowledgeMessage(queueName, clientMessage));
        }
    }

    private final class SendMessage implements ThreadBoundRunnable<String> {
        private final InternalMessage message;

        public SendMessage(InternalMessage message) {
            this.message = message;
        }

        @Override
        public void run() {
            ClientMessage clientMessage = clientSession.createMessage(message.isDurable());
            clientMessage.getBodyBuffer().writeBytes(message.toByteArray());
            clientMessage.putStringProperty("routingKey", routingKey);
            // use the duplicate detection from ActiveMQ
            clientMessage.putBytesProperty(HDR_DUPLICATE_DETECTION_ID, toByteArray(message.getId()));
            // set timeout if needed
            if(message.getTimeout() >= 0) {
                clientMessage.setExpiration(System.currentTimeMillis() + message.getTimeout());
            }
            try {
                producer.send(clientMessage);
            } catch (ActiveMQException e) {
                throw new MessageDeliveryException("IOException while publishing message",e,false);
            } /*catch(SomeRecoverableException e) { @todo: figure out which exceptions are recoverable
                this.recovering.set(true);
                throw new MessageDeliveryException("MessagingService is recovering",true);
            } */
        }

        @Override
        public String getKey() {
            return queueName;
        }
    }

    private static final class AcknowledgeMessage implements ThreadBoundRunnable<String> {
        private final String queueName;
        private final ClientMessage clientMessage;

        public AcknowledgeMessage(String queueName, ClientMessage clientMessage) {
            this.queueName = queueName;
            this.clientMessage = clientMessage;
        }

        @Override
        public void run() {
            try {
                this.clientMessage.individualAcknowledge();
            } catch (ActiveMQException e) {
                logger.error("Exception while acking message", e);
            }
        }

        @Override
        public String getKey() {
            return queueName;
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
            logger.error("Error handling transient message, payloadClass [{}]", message.getPayloadClass(),exception);
        }

        @Override
        public void onDone(InternalMessage message) {
            // do nothing
        }
    }

    private final class DestroyQueue implements ThreadBoundRunnable<String> {
        private final String queueName;

        private DestroyQueue(String queueName) {
            this.queueName = queueName;
        }

        @Override
        public void run() {
            running = false;
            if(useMessageHandler) {
                destroyLatch.countDown();
            }
        }

        @Override
        public String getKey() {
            return queueName;
        }
    }

}
