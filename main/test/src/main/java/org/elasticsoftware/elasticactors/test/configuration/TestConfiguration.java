/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.test.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.elasticsoftware.elasticactors.ActorSystemConfiguration;
import org.elasticsoftware.elasticactors.InternalActorSystemConfiguration;
import org.elasticsoftware.elasticactors.ManagedActorsRegistry;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.RemoteActorSystemConfiguration;
import org.elasticsoftware.elasticactors.base.serialization.ObjectMapperBuilder;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerService;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.cluster.HashingNodeSelectorFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.cluster.LocalActorSystemInstance;
import org.elasticsoftware.elasticactors.cluster.NodeSelectorFactory;
import org.elasticsoftware.elasticactors.cluster.logging.LoggingSettings;
import org.elasticsoftware.elasticactors.cluster.metrics.MeterConfiguration;
import org.elasticsoftware.elasticactors.cluster.metrics.MeterTagCustomizer;
import org.elasticsoftware.elasticactors.cluster.metrics.MetricsSettings;
import org.elasticsoftware.elasticactors.cluster.scheduler.SimpleScheduler;
import org.elasticsoftware.elasticactors.configuration.NodeConfiguration;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.runtime.ActorLifecycleListenerScanner;
import org.elasticsoftware.elasticactors.runtime.DefaultConfiguration;
import org.elasticsoftware.elasticactors.runtime.DefaultRemoteConfiguration;
import org.elasticsoftware.elasticactors.runtime.ManagedActorsScanner;
import org.elasticsoftware.elasticactors.runtime.MessagesScanner;
import org.elasticsoftware.elasticactors.runtime.PluggableMessageHandlersScanner;
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;
import org.elasticsoftware.elasticactors.serialization.SystemSerializationFramework;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateListener;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.DefaultActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.NoopActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.test.TestInternalActorSystems;
import org.elasticsoftware.elasticactors.test.cluster.NoopActorSystemEventRegistryService;
import org.elasticsoftware.elasticactors.test.cluster.SingleNodeClusterService;
import org.elasticsoftware.elasticactors.test.state.LoggingActorStateUpdateListener;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutorBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScans;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.aspectj.EnableSpringConfigured;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.annotation.AsyncConfigurerSupport;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * @author Joost van de Wijgerd
 */
@Configuration
@EnableSpringConfigured
@Import({
        BackplaneConfiguration.class,
        MessagingConfiguration.class,
        ClientConfiguration.class
})
@ComponentScans({
    @ComponentScan("org.elasticsoftware.elasticactors.tracing.spring"),
})
@PropertySource(value = "classpath:/system.properties", ignoreResourceNotFound = true)
public class TestConfiguration extends AsyncConfigurerSupport {
    private final PhysicalNode localNode = new PhysicalNode(UUIDTools.createRandomUUID().toString(), InetAddress.getLoopbackAddress(), true);
    private final NodeConfiguration nodeConfiguration = new NodeConfiguration();

    @Override
    @Bean(name = "asyncExecutor")
    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(Runtime.getRuntime().availableProcessors());
        executor.setMaxPoolSize(Runtime.getRuntime().availableProcessors() * 3);
        executor.setQueueCapacity(1024);
        executor.setThreadNamePrefix("ASYNCHRONOUS-ANNOTATION-EXECUTOR-");
        executor.initialize();
        return executor;
    }

    @Bean(name = "systemInitializer")
    public SystemInitializer createSystemInitializer(LocalActorSystemInstance localActorSystemInstance, ClusterService clusterService) {
        return new SystemInitializer(localNode, localActorSystemInstance, clusterService);
    }

    @Bean(name = {"internalActorSystem"}, destroyMethod = "shutdown")
    @DependsOn({
        "messageHandlersRegistry",
        "managedActorsRegistry",
        "actorLifecycleListenerRegistry"
    })
    public LocalActorSystemInstance createLocalActorSystemInstance(
        InternalActorSystems internalActorSystems,
        @Qualifier("actorSystemConfiguration") InternalActorSystemConfiguration configuration,
        NodeSelectorFactory nodeSelectorFactory,
        ManagedActorsRegistry managedActorsRegistry)
    {
        return new LocalActorSystemInstance(
            localNode,
            internalActorSystems,
            configuration,
            nodeSelectorFactory,
            managedActorsRegistry
        );
    }

    @Bean(name = {"actorSystems", "actorRefFactory", "serializationFrameworks"})
    public TestInternalActorSystems createInternalActorSystems(
            ApplicationContext applicationContext,
            ClusterService clusterService) {
        return new TestInternalActorSystems(applicationContext, clusterService, localNode);
    }

    @Bean(name = {"actorSystemConfiguration"})
    public InternalActorSystemConfiguration getConfiguration(
        ResourceLoader resourceLoader,
        Environment env) throws IOException
    {
        // get the yaml resource
        Resource configResource = resourceLoader.getResource(env.getProperty("ea.node.config.location","classpath:ea-test.yaml"));
        // yaml mapper
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        return objectMapper.readValue(configResource.getInputStream(), DefaultConfiguration.class);
    }

    @Bean(name = {"remoteConfiguration"})
    public RemoteActorSystemConfiguration getRemoteConfiguration(ActorSystemConfiguration configuration) throws IOException {
        return new DefaultRemoteConfiguration(
            "testCluster",
            configuration.getName(),
            configuration.getNumberOfShards(),
            configuration.getQueuesPerShard(),
            configuration.getShardHashSeed(),
            configuration.getMultiQueueHashSeed()
        );
    }

    @Bean(name = {"objectMapperBuilder"})
    public ObjectMapperBuilder createObjectMapperBuilder(
            SimpleScheduler simpleScheduler,
            TestInternalActorSystems actorRefFactory) {
        // @todo: fix version
        return new ObjectMapperBuilder(actorRefFactory, simpleScheduler, "1.0.0");
    }

    @Bean(name = {"objectMapper"})
    public ObjectMapper createObjectMapper(ObjectMapperBuilder builder) {
        return builder.build();
    }

    @Bean(name = {"managedActorsRegistry"})
    public ManagedActorsScanner createManagedActorsScanner(ApplicationContext applicationContext) {
        return new ManagedActorsScanner(applicationContext);
    }

    @Bean(name = {"messagesScanner"})
    public MessagesScanner createMessageScanner(ApplicationContext applicationContext) {
        return new MessagesScanner(applicationContext);
    }

    @Bean(name = {"messageHandlersRegistry"})
    public PluggableMessageHandlersScanner createPluggableMessagesHandlersScanner(ApplicationContext applicationContext) {
        return new PluggableMessageHandlersScanner(applicationContext);
    }

    @Bean(name = {"actorLifecycleListenerRegistry"})
    public ActorLifecycleListenerScanner createActorLifecycleListenerScanner(ApplicationContext applicationContext) {
        return new ActorLifecycleListenerScanner(applicationContext);
    }

    @Bean(name = {"nodeSelectorFactory"})
    public NodeSelectorFactory getNodeSelectorFactory() {
        return new HashingNodeSelectorFactory();
    }

    @Bean(name = {"nodeActorCacheManager"})
    public NodeActorCacheManager createNodeActorCacheManager(
        Environment env,
        @Nullable @Qualifier("elasticActorsMeterRegistry") MeterRegistry meterRegistry,
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MeterTagCustomizer tagCustomizer)
    {
        int maximumSize = env.getProperty("ea.nodeCache.maximumSize",Integer.class,10240);
        return new NodeActorCacheManager(
            maximumSize,
            MeterConfiguration.build(env, meterRegistry, "nodeActorCache", tagCustomizer)
        );
    }

    @Bean(name = {"shardActorCacheManager"})
    public ShardActorCacheManager createShardActorCacheManager(
        Environment env,
        @Nullable @Qualifier("elasticActorsMeterRegistry") MeterRegistry meterRegistry,
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MeterTagCustomizer tagCustomizer)
    {
        int maximumSize = env.getProperty("ea.shardCache.maximumSize",Integer.class,10240);
        return new ShardActorCacheManager(
            maximumSize,
            MeterConfiguration.build(env, meterRegistry, "shardActorCache", tagCustomizer)
        );
    }

    @Bean(name = {"elasticActorsMeterRegistry"})
    public MeterRegistry createMeterRegistry() {
        return new SimpleMeterRegistry();
    }

    @Bean(name = {"actorExecutor"}, destroyMethod = "shutdown")
    public ThreadBoundExecutor createActorExecutor(
        Environment env,
        @Nullable @Qualifier("elasticActorsMeterRegistry") MeterRegistry meterRegistry,
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MeterTagCustomizer tagCustomizer)
    {
        return ThreadBoundExecutorBuilder.build(
            env,
            "actorExecutor",
            "ACTOR-WORKER",
            meterRegistry,
            tagCustomizer
        );
    }

    @Bean(name = {"queueExecutor"}, destroyMethod = "shutdown")
    @DependsOn("actorExecutor")
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

    @Bean(name = {"scheduler"})
    public SimpleScheduler createScheduler(
        Environment env,
        @Nullable @Qualifier("elasticActorsMeterRegistry") MeterRegistry meterRegistry,
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MeterTagCustomizer tagCustomizer)
    {
        return new SimpleScheduler(MeterConfiguration.build(
            env,
            meterRegistry,
            "scheduler",
            tagCustomizer
        ));
    }

    @Bean(name= "clusterService")
    public ClusterService createClusterService() {
        return new SingleNodeClusterService(this.localNode);
    }

    @Bean(name = {"actorSystemEventListenerService"})
    public ActorSystemEventListenerService createActorSystemEventListenerService() {
        return new NoopActorSystemEventRegistryService();
    }

    @Bean(name = {"actorStateUpdateProcessor"})
    public ActorStateUpdateProcessor createActorStateUpdateProcessor(
        Environment env,
        List<ActorStateUpdateListener> listeners,
        @Nullable @Qualifier("elasticActorsMeterRegistry") MeterRegistry meterRegistry,
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MeterTagCustomizer tagCustomizer)
    {
        if(listeners.isEmpty()) {
            return new NoopActorStateUpdateProcessor();
        } else {
            return new DefaultActorStateUpdateProcessor(
                listeners,
                env,
                1,
                20,
                meterRegistry,
                tagCustomizer
            );
        }
    }

    @Bean(name = {"loggingActorStateUpdateListener"})
    public LoggingActorStateUpdateListener createLoggingActorStateUpdateListener() {
        return new LoggingActorStateUpdateListener();
    }

    @Bean(name = "systemSerializationFramework")
    public SystemSerializationFramework createSystemSerializationFramework(SerializationFrameworks serializationFrameworks) {
        return new SystemSerializationFramework(serializationFrameworks);
    }

    @Bean(name = "nodeMetricsSettings")
    public MetricsSettings nodeMetricsSettings(Environment environment) {
        return nodeConfiguration.nodeMetricsSettings(environment);
    }

    @Bean(name = "shardMetricsSettings")
    public MetricsSettings shardMetricsSettings(Environment environment) {
        return nodeConfiguration.shardMetricsSettings(environment);
    }

    @Bean(name = "nodeLoggingSettings")
    public LoggingSettings nodeLoggingSettings(Environment environment) {
        return nodeConfiguration.nodeLoggingSettings(environment);
    }

    @Bean(name = "shardLoggingSettings")
    public LoggingSettings shardLoggingSettings(Environment environment) {
        return nodeConfiguration.shardLoggingSettings(environment);
    }
}
