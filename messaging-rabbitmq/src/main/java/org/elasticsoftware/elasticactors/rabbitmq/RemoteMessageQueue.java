/*
 * Copyright 2013 - 2017 The Original Authors
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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import net.jodah.lyra.event.DefaultChannelListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Joost van de Wijgerd
 */
public final class RemoteMessageQueue extends DefaultChannelListener implements MessageQueue {
    private final Logger logger;
    private final Channel producerChannel;
    private final String exchangeName;
    private final String queueName;
    private final ChannelListenerRegistry channelListenerRegistry;
    private final AtomicBoolean recovering = new AtomicBoolean(false);

    public RemoteMessageQueue(ChannelListenerRegistry channelListenerRegistry,Channel producerChannel, String exchangeName, String queueName) {
        this.producerChannel = producerChannel;
        this.exchangeName = exchangeName;
        this.queueName = queueName;
        this.logger = LogManager.getLogger(String.format("Producer[%s->%s]",exchangeName,queueName));
        this.channelListenerRegistry = channelListenerRegistry;
        this.channelListenerRegistry.addChannelListener(this.producerChannel,this);
    }

    @Override
    public boolean offer(InternalMessage message) {
        // see if we are recovering first
        if(this.recovering.get()) {
            throw new MessageDeliveryException("MessagingService is recovering",true);
        }
        try {
            final AMQP.BasicProperties props = createProps(message);
            producerChannel.basicPublish(exchangeName, queueName,false,false,props,message.toByteArray());
            return true;
        } catch (IOException e) {
            throw new MessageDeliveryException("IOException while publishing message",e,false);
        } catch(AlreadyClosedException e) {
            this.recovering.set(true);
            throw new MessageDeliveryException("MessagingService is recovering",true);
        }
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
        // nothing to do
    }

    @Override
    public void destroy() {
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
}
