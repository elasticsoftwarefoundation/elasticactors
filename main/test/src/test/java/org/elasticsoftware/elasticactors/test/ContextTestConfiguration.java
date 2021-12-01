package org.elasticsoftware.elasticactors.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.base.serialization.ObjectMapperBuilder;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerService;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.configuration.NodeConfiguration;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactoryFactory;
import org.elasticsoftware.elasticactors.test.cluster.NoopActorSystemEventRegistryService;
import org.elasticsoftware.elasticactors.test.cluster.SingleNodeClusterService;
import org.elasticsoftware.elasticactors.test.cluster.UnsupportedThreadBoundExecutor;
import org.elasticsoftware.elasticactors.test.configuration.BackplaneConfiguration;
import org.elasticsoftware.elasticactors.test.configuration.MessagingConfiguration;
import org.elasticsoftware.elasticactors.test.configuration.TestConfiguration;
import org.elasticsoftware.elasticactors.test.messaging.UnsupportedMessageQueueFactoryFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

import java.net.InetAddress;

@Import({NodeConfiguration.class, BackplaneConfiguration.class, MessagingConfiguration.class})
@ComponentScan(excludeFilters = {
    @ComponentScan.Filter(
        type = FilterType.ASSIGNABLE_TYPE,
        classes = TestConfiguration.class
    )
})
@PropertySource(value = "classpath:/system.properties")
public class ContextTestConfiguration {

    @Bean(name= "clusterService")
    public ClusterService createClusterService() {
        return new SingleNodeClusterService(new PhysicalNode(
            "test-0",
            InetAddress.getLoopbackAddress(),
            true
        ));
    }

    @Bean(name = {"actorSystemEventListenerService"})
    public ActorSystemEventListenerService createActorSystemEventListenerService() {
        return new NoopActorSystemEventRegistryService();
    }

    @Bean(name = {"objectMapper"})
    public ObjectMapper createObjectMapper(ObjectMapperBuilder builder) {
        return builder.build();
    }

    @Bean(name = {"remoteActorSystemMessageQueueFactoryFactory"})
    public MessageQueueFactoryFactory getRemoteActorSystemMessageQueueFactoryFactory() {
        return new UnsupportedMessageQueueFactoryFactory();
    }

    @Bean(name = {"asyncUpdateExecutor"})
    public ThreadBoundExecutor createAsyncUpdateExecutor() {
        return new UnsupportedThreadBoundExecutor();
    }

    @Bean(name = "elasticActorsMeterRegistry")
    public MeterRegistry createMeterRegistry() {
        return new SimpleMeterRegistry();
    }

}
