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

package org.elasticsoftware.elasticactors.client.spring;

import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

public class RabbitTemplateMessageQueueFactory implements MessageQueueFactory {

    private static final String QUEUE_NAME_FORMAT = "%s/%s";
    private static final String EA_EXCHANGE_FORMAT = "ea.%s";

    private final String elasticActorsCluster;
    private final String exchangeName;
    private final AmqpAdmin amqpAdmin;
    private final RabbitTemplate rabbitTemplate;

    public RabbitTemplateMessageQueueFactory(
            String elasticActorsCluster,
            AmqpAdmin amqpAdmin,
            RabbitTemplate rabbitTemplate) {
        this.elasticActorsCluster = elasticActorsCluster;
        this.amqpAdmin = amqpAdmin;
        this.rabbitTemplate = rabbitTemplate;
        this.exchangeName = String.format(EA_EXCHANGE_FORMAT, elasticActorsCluster);
    }

    @Override
    public MessageQueue create(String name, MessageHandler messageHandler) throws Exception {
        String queueName = String.format(QUEUE_NAME_FORMAT, elasticActorsCluster, name);
        ensureQueueExists(queueName);
        RabbitTemplateMessageQueue messageQueue =
            new RabbitTemplateMessageQueue(exchangeName, queueName, rabbitTemplate);
        messageQueue.initialize();
        return messageQueue;
    }

    private void ensureQueueExists(String queueName) {
        // ensure we have the queue created on the broker
        Queue queue = QueueBuilder.durable(queueName).build();
        amqpAdmin.declareQueue(queue);
        // and bound to the exchange
        DirectExchange exchange = ExchangeBuilder.directExchange(exchangeName).build();
        amqpAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(queueName));
    }
}
