package org.elasticsoftware.elasticactors.client.spring;

import org.elasticsoftware.elasticactors.client.configuration.ClientConfiguration;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@Import(ClientConfiguration.class)
public class SpringAMQPClientConfiguration {

    @Bean
    public RabbitTemplateMessageQueueFactoryFactory rabbitTemplateMessageQueueFactoryFactory(
            AmqpAdmin amqpAdmin,
            RabbitTemplate rabbitTemplate) {
        return new RabbitTemplateMessageQueueFactoryFactory(amqpAdmin, rabbitTemplate);
    }

}
