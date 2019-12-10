package org.elasticsoftware.elasticactors.client.spring;

import org.elasticsoftware.elasticactors.client.configuration.ClientConfiguration;
import org.elasticsoftware.elasticactors.configuration.MessagingConfiguration;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutorImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;

import static java.lang.Boolean.FALSE;

/**
 * Convenience class for importing the whole configuration needed for creating a write-only client
 * with the default RabbitMQ messaging layer.
 */
@Import({ClientConfiguration.class, MessagingConfiguration.class})
public class RabbitClientConfiguration {

    @Bean(name = {"queueExecutor"}, destroyMethod = "shutdown")
    public ThreadBoundExecutor createQueueExecutor(Environment env) {
        final int workers = env.getProperty(
                "ea.queueExecutor.workerCount",
                Integer.class,
                Runtime.getRuntime().availableProcessors() * 3);
        final Boolean useDisruptor =
                env.getProperty("ea.actorExecutor.useDisruptor", Boolean.class, FALSE);
        if (useDisruptor) {
            return new org.elasticsoftware.elasticactors.util.concurrent.disruptor.ThreadBoundExecutorImpl(
                    new DaemonThreadFactory("QUEUE-WORKER"),
                    workers);
        } else {
            return new ThreadBoundExecutorImpl(new DaemonThreadFactory("QUEUE-WORKER"), workers);
        }
    }

}
