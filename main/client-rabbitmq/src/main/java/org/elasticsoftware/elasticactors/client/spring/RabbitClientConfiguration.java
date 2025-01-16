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

import io.micrometer.core.instrument.MeterRegistry;
import org.elasticsoftware.elasticactors.client.configuration.ClientConfiguration;
import org.elasticsoftware.elasticactors.cluster.metrics.MicrometerTagCustomizer;
import org.elasticsoftware.elasticactors.configuration.MessagingConfiguration;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutorBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.lang.Nullable;

/**
 * Convenience class for importing the whole configuration needed for creating a write-only client
 * with the default RabbitMQ messaging layer.
 */
@Import({ClientConfiguration.class, MessagingConfiguration.class})
public class RabbitClientConfiguration {

    @Bean(name = {"queueExecutor"}, destroyMethod = "shutdown")
    public ThreadBoundExecutor createQueueExecutor(
        Environment env,
        @Nullable @Qualifier("elasticActorsMeterRegistry") MeterRegistry meterRegistry,
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MicrometerTagCustomizer tagCustomizer)
    {
        return ThreadBoundExecutorBuilder.build(
            env,
            "queueExecutor",
            "QUEUE-WORKER",
            meterRegistry,
            tagCustomizer
        );
    }

}
