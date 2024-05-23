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

package org.elasticsoftware.elasticactors.rabbitmq.health;

import org.elasticsoftware.elasticactors.health.HealthCheck;
import org.elasticsoftware.elasticactors.health.HealthCheckResult;
import org.elasticsoftware.elasticactors.rabbitmq.RabbitMQMessagingService;
import org.springframework.beans.factory.annotation.Autowired;

import static org.elasticsoftware.elasticactors.health.HealthCheckResult.healthy;
import static org.elasticsoftware.elasticactors.health.HealthCheckResult.unhealthy;

/**
 * @author Rob de Boer
 */
public class RabbitMQHealthCheck implements HealthCheck {

    private final RabbitMQMessagingService rabbitMQMessagingService;

    @Autowired
    public RabbitMQHealthCheck(RabbitMQMessagingService rabbitMQMessagingService) {
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
