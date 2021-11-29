package org.elasticsoftware.elasticactors.client.spring;

import io.micrometer.core.instrument.MeterRegistry;
import org.elasticsoftware.elasticactors.client.configuration.ClientConfiguration;
import org.elasticsoftware.elasticactors.cluster.metrics.MeterTagCustomizer;
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
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MeterTagCustomizer tagCustomizer)
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
