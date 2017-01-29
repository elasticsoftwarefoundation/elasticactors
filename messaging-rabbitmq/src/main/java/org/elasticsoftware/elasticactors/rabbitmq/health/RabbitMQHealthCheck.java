package org.elasticsoftware.elasticactors.rabbitmq.health;

import org.elasticsoftware.elasticactors.health.HealthCheck;
import org.elasticsoftware.elasticactors.health.HealthCheckResult;
import org.elasticsoftware.elasticactors.rabbitmq.RabbitMQMessagingServiceInterface;
import org.springframework.beans.factory.annotation.Autowired;

import static org.elasticsoftware.elasticactors.health.HealthCheckResult.healthy;
import static org.elasticsoftware.elasticactors.health.HealthCheckResult.unhealthy;

/**
 * @author Rob de Boer
 */
public class RabbitMQHealthCheck implements HealthCheck {

    private final RabbitMQMessagingServiceInterface rabbitMQMessagingService;

    @Autowired
    public RabbitMQHealthCheck(RabbitMQMessagingServiceInterface rabbitMQMessagingService) {
        this.rabbitMQMessagingService = rabbitMQMessagingService;
    }

    @Override
    public HealthCheckResult check() {
        if (!rabbitMQMessagingService.isClientConnectionOpen()) {
            return unhealthy("RabbitMQ is not connected; the client connection is closed");
        } else if (!rabbitMQMessagingService.areConsumerChannelsOpen()) {
            return unhealthy("RabbitMQ is not connected; the consumer channel(s) is(are) closed");
        } else if (!rabbitMQMessagingService.areProducerChannelsOpen()) {
            return unhealthy("RabbitMQ is not connected; the producer channel(s) is(are) closed");
        }

        return healthy();
    }
}
