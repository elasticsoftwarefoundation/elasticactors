/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.rabbitmq.cpt;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import net.jodah.lyra.event.DefaultChannelListener;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.rabbitmq.ChannelListenerRegistry;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Joost van de Wijgerd
 */
final class RemoteMessageQueue extends DefaultChannelListener implements MessageQueue {

    private final static Logger logger = LoggerFactory.getLogger(RemoteMessageQueue.class);

    private final Channel producerChannel;
    private final String exchangeName;
    private final String queueName;
    private final ChannelListenerRegistry channelListenerRegistry;
    private final ThreadBoundExecutor queueExecutor;
    private final AtomicBoolean recovering = new AtomicBoolean(false);

    public RemoteMessageQueue(ChannelListenerRegistry channelListenerRegistry, ThreadBoundExecutor queueExecutor, Channel producerChannel, String exchangeName, String queueName) {
        this.queueExecutor = queueExecutor;
        this.producerChannel = producerChannel;
        this.exchangeName = exchangeName;
        this.queueName = queueName;
        this.channelListenerRegistry = channelListenerRegistry;
        this.channelListenerRegistry.addChannelListener(this.producerChannel,this);
    }

    @Override
    public boolean offer(InternalMessage message) {
        // see if we are recovering first
        if(this.recovering.get()) {
            throw new MessageDeliveryException("MessagingService is recovering",true);
        }
        queueExecutor.execute(new MessageSender(message));
        return true;
    }

    private AMQP.BasicProperties createProps(InternalMessage message) {
        if(message.getTimeout() < 0) {
            return message.isDurable() ? MessageProperties.PERSISTENT_BASIC : MessageProperties.BASIC;
        } else {
            if(message.isDurable()) {
                return new AMQP.BasicProperties.Builder().contentType("application/octet-stream").deliveryMode(2)
                        .priority(0).expiration(String.valueOf(message.getTimeout())).build();
            } else {
                return new AMQP.BasicProperties.Builder().contentType("application/octet-stream").deliveryMode(1)
                        .priority(0).expiration(String.valueOf(message.getTimeout())).build();
            }
        }
    }

    @Override
    public boolean add(InternalMessage message) {
        return offer(message);
    }

    @Override
    public InternalMessage poll() {
        throw new UnsupportedOperationException("Remote queues cannot be polled");
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public void initialize() throws Exception {
        logger.info("Starting remote message queue [{}->{}]", exchangeName, queueName);
    }

    @Override
    public void destroy() {
        logger.info("Stopping remote message queue [{}->{}]", exchangeName, queueName);
        this.channelListenerRegistry.removeChannelListener(this.producerChannel,this);
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

    private final class MessageSender implements ThreadBoundRunnable<String> {
        private final InternalMessage message;

        public MessageSender(InternalMessage message) {
            this.message = message;
        }

        @Override
        public void run() {
            try {
                final AMQP.BasicProperties props = createProps(message);
                producerChannel.basicPublish(exchangeName, queueName, false, false, props, message.toByteArray());
            } catch (IOException e) {
                logger.error("IOException while publishing message", e);
            } catch(AlreadyClosedException e) {
                RemoteMessageQueue.this.recovering.set(true);
                logger.error("MessagingService is recovering");
            }
        }

        @Override
        public String getKey() {
            return RemoteMessageQueue.this.queueName;
        }
    }
}
