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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class RemoteMessageQueue implements MessageQueue {
    private final Logger logger;
    private final Channel producerChannel;
    private final String exchangeName;
    private final String queueName;

    public RemoteMessageQueue(Channel producerChannel, String exchangeName, String queueName) {
        this.producerChannel = producerChannel;
        this.exchangeName = exchangeName;
        this.queueName = queueName;
        this.logger = Logger.getLogger(String.format("Producer[%s->%s]",exchangeName,queueName));
    }

    @Override
    public boolean offer(InternalMessage message) {
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
        // nothing to do, channel is reused across other remote queues as well
    }
}
