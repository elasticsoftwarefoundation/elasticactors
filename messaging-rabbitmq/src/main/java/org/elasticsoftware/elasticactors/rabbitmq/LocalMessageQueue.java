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
import net.jodah.lyra.event.ChannelListener;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class LocalMessageQueue extends DefaultConsumer implements MessageQueue, ChannelListener {
    private final Logger logger;
    private final Channel consumerChannel;
    private final Channel producerChannel;
    private final String exchangeName;
    private final String queueName;
    private final MessageHandler messageHandler;
    private final TransientAck transientAck = new TransientAck();
    private final ThreadBoundExecutor<String> queueExecutor;
    private final CountDownLatch destroyLatch = new CountDownLatch(1);
    private final InternalMessageDeserializer internalMessageDeserializer;
    private final AtomicBoolean recovering = new AtomicBoolean(false);
    private final ChannelListenerRegistry channelListenerRegistry;
    private final MessageAcker messageAcker;

    public LocalMessageQueue(ThreadBoundExecutor<String> queueExecutor,
                             ChannelListenerRegistry channelListenerRegistry,
                             Channel consumerChannel,
                             Channel producerChannel,
                             String exchangeName,
                             String queueName,
                             MessageHandler messageHandler,
                             InternalMessageDeserializer internalMessageDeserializer,
                             MessageAcker messageAcker) {
        super(consumerChannel);
        this.queueExecutor = queueExecutor;
        this.consumerChannel = consumerChannel;
        this.producerChannel = producerChannel;
        this.exchangeName = exchangeName;
        this.queueName = queueName;
        this.messageHandler = messageHandler;
        this.internalMessageDeserializer = internalMessageDeserializer;
        this.messageAcker = messageAcker;
        this.logger = Logger.getLogger(format("LocalMessageQueue[%s->%s]", exchangeName, queueName));
        this.channelListenerRegistry = channelListenerRegistry;
        this.channelListenerRegistry.addChannelListener(this.producerChannel,this);
    }

    @Override
    public boolean offer(final InternalMessage message) {
        // see if we are recovering first
        if(this.recovering.get()) {
            throw new MessageDeliveryException("MessagingService is recovering",true);
        }
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
                throw new MessageDeliveryException("IOException while publishing message",e,false);
            } catch(AlreadyClosedException e) {
                this.recovering.set(true);
                throw new MessageDeliveryException("MessagingService is recovering",true);
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
        } finally {
            this.channelListenerRegistry.removeChannelListener(this.producerChannel,this);
        }
    }

    @Override
    public void handleCancelOk(String consumerTag) {
        destroyLatch.countDown();
    }

    @Override
    public void handleCancel(String consumerTag) throws IOException {
        // we were cancelled by an outside force, should not happen. treat as an error
        logger.error(format("Unexpectedly cancelled: consumerTag = %s", consumerTag));
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
        try {
            messageAcker.deliver(envelope.getDeliveryTag());
            // execute on seperate (thread bound) executor
            queueExecutor.execute(new RabbitMQMessageHandler(queueName,body,internalMessageDeserializer,messageHandler,new RabbitMQAck(envelope),logger));
        } catch(Exception e) {
            logger.error("Unexpected Exception on handleDelivery.. Acking the message so it will not clog up the system",e);
            messageAcker.ack(envelope.getDeliveryTag());
        }
    }

    @Override
    public void onConsumerRecovery(Channel channel) {
        // ignore
    }

    @Override
    public void onCreate(Channel channel) {
        // ignore
    }

    @Override
    public void onCreateFailure(Throwable failure) {
        // ignore
    }

    @Override
    public void onRecovery(Channel channel) {
        // reset the recovery flag
        if(this.recovering.compareAndSet(true,false)) {
            logger.info("RabbitMQ Channel recovered");
        }
    }

    @Override
    public void onRecoveryFailure(Channel channel, Throwable failure) {
        // log an error
        logger.error("RabbitMQ Channel recovery failed");
    }

    private static final class RabbitMQMessageHandler implements ThreadBoundRunnable<String> {
        private final String queueName;
        private final InternalMessageDeserializer internalMessageDeserializer;
        private final byte[] body;
        private final MessageHandler messageHandler;
        private final MessageHandlerEventListener listener;
        private final Logger logger;

        private RabbitMQMessageHandler(String queueName, byte[] body, InternalMessageDeserializer internalMessageDeserializer, MessageHandler messageHandler, MessageHandlerEventListener listener, Logger logger) {
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
            try {
                // get the body data
                final InternalMessage message = internalMessageDeserializer.deserialize(body);
                messageHandler.handleMessage(message,listener);
            } catch(Exception e) {
                logger.error("Unexpected exception on #handleMessage",e);
            }
        }
    }

    private final class RabbitMQAck implements MessageHandlerEventListener {
        private final Envelope envelope;
        //private final InternalMessage message;

        private RabbitMQAck(Envelope envelope) {
            this.envelope = envelope;
        }

        @Override
        public void onError(final InternalMessage message,final Throwable exception) {
            onDone(message);
        }

        @Override
        public void onDone(final InternalMessage message) {
            messageAcker.ack(envelope.getDeliveryTag());
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
