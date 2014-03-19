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

package org.elasticsoftware.elasticactors.rabbitmq;

import com.rabbitmq.client.*;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.messaging.*;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
public final class LocalMessageQueue extends DefaultConsumer implements MessageQueue {
    private final Logger logger;
    private final Channel consumerChannel;
    private final Channel producerChannel;
    private final String exchangeName;
    private final String queueName;
    private final MessageHandler messageHandler;
    private final TransientAck transientAck = new TransientAck();
    private final ThreadBoundExecutor<String> queueExecutor;
    private final CountDownLatch destroyLatch = new CountDownLatch(1);

    public LocalMessageQueue(ThreadBoundExecutor<String> queueExecutor, Channel consumerChannel, Channel producerChannel, String exchangeName, String queueName, MessageHandler messageHandler) {
        super(consumerChannel);
        this.queueExecutor = queueExecutor;
        this.consumerChannel = consumerChannel;
        this.producerChannel = producerChannel;
        this.exchangeName = exchangeName;
        this.queueName = queueName;
        this.messageHandler = messageHandler;
        this.logger = Logger.getLogger(String.format("Producer[%s->%s]",exchangeName,queueName));
    }

    @Override
    public boolean offer(final InternalMessage message) {
        if(!message.isDurable()) {
            // execute on a separate (thread bound) executor
            queueExecutor.execute(new InternalMessageHandler(queueName,message,messageHandler,transientAck,logger));
            return true;
        } else {
            try {
                final AMQP.BasicProperties props = message.isDurable() ? MessageProperties.PERSISTENT_BASIC : MessageProperties.BASIC;
                producerChannel.basicPublish(exchangeName, queueName,false,false,props,message.toByteArray());
                return true;
            } catch (IOException e) {
                // @todo: what to do with the message?
                logger.error("IOException on publish",e);
                return false;
            }
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
        // @todo: possibly loop until we are the only consumer on the queue
        consumerChannel.basicQos(0);
        consumerChannel.basicConsume(queueName,false,this);
    }

    @Override
    public void destroy() {
        try {
            consumerChannel.basicCancel(getConsumerTag());
            destroyLatch.await(4, TimeUnit.SECONDS);
        } catch (IOException e) {
            logger.error("IOException while cancelling consumer",e);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Override
    public void handleCancelOk(String consumerTag) {
        destroyLatch.countDown();
    }

    @Override
    public void handleCancel(String consumerTag) throws IOException {
        // we were cancelled by an outside force, should not happen. treat as an error
        logger.error(String.format("Unexpectedly cancelled: consumerTag = %s",consumerTag));
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        // @todo: some kind of higher level error handling
    }

    @Override
    public void handleRecoverOk(String consumerTag) {

    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        // get the body data
        final InternalMessage message = InternalMessageDeserializer.get().deserialize(body);
        // execute on seperate (thread bound) executor
        queueExecutor.execute(new InternalMessageHandler(queueName,message,messageHandler,new RabbitMQAck(envelope,message),logger));
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

    private final class RabbitMQAck implements MessageHandlerEventListener {
        private final Envelope envelope;
        private final InternalMessage message;

        private RabbitMQAck(Envelope envelope, InternalMessage message) {
            this.envelope = envelope;
            this.message = message;
        }

        @Override
        public void onError(final InternalMessage message,final Throwable exception) {
            logger.error("Exception while handling message, acking anyway",exception);
            onDone(message);
        }

        @Override
        public void onDone(final InternalMessage message) {
            if(this.message != message) {
                throw new IllegalArgumentException(String.format("Trying to ack wrong message: expected %d, got %d ",
                                                                 this.message.hashCode(),message.hashCode()));
            }
            try {
                consumerChannel.basicAck(envelope.getDeliveryTag(),false);
            } catch (IOException e) {
                logger.error("Exception while acking message",e);
            }
        }
    }

    private final class TransientAck implements MessageHandlerEventListener {

        @Override
        public void onError(InternalMessage message, Throwable exception) {
            logger.error(String.format("Error handling transient message, payloadClass [%s]",message.getPayloadClass()),exception);
        }

        @Override
        public void onDone(InternalMessage message) {
            // do nothing
        }
    }
}
