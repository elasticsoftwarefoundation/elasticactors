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

package org.elasticsoftware.elasticactors.client.spring;

import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

public class RabbitTemplateMessageQueue implements MessageQueue {

    private final static Logger logger = LoggerFactory.getLogger(RabbitTemplateMessageQueue.class);

    private final String exchange;
    private final String routingKey;
    private final RabbitTemplate rabbitTemplate;

    public RabbitTemplateMessageQueue(
            String exchange,
            String routingKey,
            RabbitTemplate rabbitTemplate) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.rabbitTemplate = rabbitTemplate;
    }

    private MessageProperties createProps(InternalMessage message) {
        MessageBuilderSupport<MessageProperties> messageBuilderSupport =
                MessagePropertiesBuilder.newInstance()
                        .setContentType(MessageProperties.CONTENT_TYPE_BYTES)
                        .setDeliveryMode(message.isDurable()
                                ? MessageDeliveryMode.PERSISTENT
                                : MessageDeliveryMode.NON_PERSISTENT);
        if (message.getTimeout() < 0) {
            return messageBuilderSupport.build();
        } else {
            return messageBuilderSupport
                    .setPriority(MessageProperties.DEFAULT_PRIORITY)
                    .setExpiration(String.valueOf(message.getTimeout()))
                    .build();
        }
    }

    /**
     * As RabbitTemplate is asynchronous, it's recommended to set the channel as transacted or use
     * publisher confirmations in order to make sure that the message was indeed published. Using
     * transacted channel will make this method block until the broker has confirmed the publishing.
     * Publisher confirmations won't block, but you have to do the checks yourself.
     */
    @Override
    public boolean offer(InternalMessage message) {
        try {
            rabbitTemplate.send(
                    exchange,
                    routingKey,
                    MessageBuilder.withBody(message.toByteArray())
                            .andProperties(createProps(message))
                            .build());
        } catch (Exception e) {
            throw new MessageDeliveryException(
                    "Unexpected exception while sending message using RabbitTemplate",
                    e,
                    false);
        }
        return true;
    }

    /**
     * An alias for {@link RabbitTemplateMessageQueue#offer(InternalMessage)}
     */
    @Override
    public boolean add(InternalMessage message) {
        return offer(message);
    }

    @Override
    public InternalMessage poll() {
        throw new UnsupportedOperationException("Client message queues cannot poll messages");
    }

    @Override
    public String getName() {
        return routingKey;
    }

    @Override
    public void initialize() throws Exception {
        logger.info("Starting remote message queue [{}->{}]", exchange, routingKey);
    }

    @Override
    public void destroy() {
        logger.info("Stopping remote message queue [{}->{}]", exchange, routingKey);
    }
}
