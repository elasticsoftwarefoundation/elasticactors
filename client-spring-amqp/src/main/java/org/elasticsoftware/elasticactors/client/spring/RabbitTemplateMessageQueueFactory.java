package org.elasticsoftware.elasticactors.client.spring;

import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
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
        return new RabbitTemplateMessageQueue(exchangeName, queueName, rabbitTemplate);
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
