package org.elasticsoftware.elasticactors.client.spring;

import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactoryFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

public class RabbitTemplateMessageQueueFactoryFactory implements MessageQueueFactoryFactory {

    private final AmqpAdmin amqpAdmin;
    private final RabbitTemplate rabbitTemplate;

    public RabbitTemplateMessageQueueFactoryFactory(
            AmqpAdmin amqpAdmin,
            RabbitTemplate rabbitTemplate) {
        this.amqpAdmin = amqpAdmin;
        this.rabbitTemplate = rabbitTemplate;
    }

    @Override
    public MessageQueueFactory create(String clusterName) {
        return new RabbitTemplateMessageQueueFactory(clusterName, amqpAdmin, rabbitTemplate);
    }
}
