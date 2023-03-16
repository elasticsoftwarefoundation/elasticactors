/*
 * Copyright 2013 - 2023 The Original Authors
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

package org.elasticsoftware.elasticactors.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.GuavaCacheMetrics;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.InternalActorSystemConfiguration;
import org.elasticsoftware.elasticactors.ManagedActorsRegistry;
import org.elasticsoftware.elasticactors.base.serialization.ObjectMapperBuilder;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerRepository;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerService;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventRegistryImpl;
import org.elasticsoftware.elasticactors.cluster.HashingNodeSelectorFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.LocalActorSystemInstance;
import org.elasticsoftware.elasticactors.cluster.NodeSelectorFactory;
import org.elasticsoftware.elasticactors.cluster.RemoteActorSystems;
import org.elasticsoftware.elasticactors.cluster.logging.LoggingSettings;
import org.elasticsoftware.elasticactors.cluster.metrics.MetricsSettings;
import org.elasticsoftware.elasticactors.cluster.metrics.MicrometerConfiguration;
import org.elasticsoftware.elasticactors.cluster.metrics.MicrometerTagCustomizer;
import org.elasticsoftware.elasticactors.cluster.scheduler.ShardedScheduler;
import org.elasticsoftware.elasticactors.health.InternalActorSystemHealthCheck;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactoryFactory;
import org.elasticsoftware.elasticactors.runtime.ActorLifecycleListenerScanner;
import org.elasticsoftware.elasticactors.runtime.DefaultConfiguration;
import org.elasticsoftware.elasticactors.runtime.ElasticActorsNode;
import org.elasticsoftware.elasticactors.runtime.ManagedActorsScanner;
import org.elasticsoftware.elasticactors.runtime.MessagesScanner;
import org.elasticsoftware.elasticactors.runtime.PluggableMessageHandlersScanner;
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;
import org.elasticsoftware.elasticactors.serialization.SystemSerializationFramework;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateListener;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.DefaultActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.NoopActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutorBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScans;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.lang.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import static java.lang.Boolean.FALSE;

/**
 * @author Joost van de Wijgerd
 */
@ComponentScans({
    @ComponentScan("org.elasticsoftware.elasticactors.tracing.spring"),
})
public class NodeConfiguration {

    @Bean(name = {
        "elasticActorsNode",
        "actorSystems",
        "actorRefFactory",
        "serializationFrameworks"
    })
    @DependsOn({
        "messageHandlersRegistry",
        "managedActorsRegistry",
        "actorLifecycleListenerRegistry"
    })
    public ElasticActorsNode createElasticActorsNode(
        Environment env,
        @Qualifier("actorRefCache") Cache<String, ActorRef> actorRefCache)
        throws UnknownHostException
    {
        String nodeId = env.getRequiredProperty("ea.node.id");
        InetAddress nodeAddress = InetAddress.getByName(env.getRequiredProperty("ea.node.address"));
        String clusterName = env.getRequiredProperty("ea.cluster");
        return new ElasticActorsNode(clusterName, nodeId, nodeAddress, actorRefCache);
    }

    @Bean(name = {"actorRefCache"})
    public Cache<String, ActorRef> createActorRefCache(
        Environment env,
        @Nullable @Qualifier("elasticActorsMeterRegistry") MeterRegistry meterRegistry,
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MicrometerTagCustomizer tagCustomizer)
    {
        int maximumSize = env.getProperty("ea.actorRefCache.maximumSize", Integer.class, 10240);
        MicrometerConfiguration configuration =
            MicrometerConfiguration.build(env, meterRegistry, "actorRefCache", tagCustomizer);
        CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().maximumSize(maximumSize);
        if (configuration != null) {
            builder.recordStats();
            return GuavaCacheMetrics.monitor(
                configuration.getRegistry(),
                builder.build(),
                configuration.getComponentName(),
                configuration.getTags()
            );
        } else {
            return builder.build();
        }
    }

    @Bean(name = {"actorSystemConfiguration"})
    public InternalActorSystemConfiguration createConfiguration(
        ResourceLoader resourceLoader,
        Environment env) throws IOException
    {
        // get the yaml resource
        Resource configResource = resourceLoader.getResource(env.getProperty(
            "ea.node.config.location",
            "classpath:ea-default.yaml"
        ));
        // yaml mapper
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        return objectMapper.readValue(configResource.getInputStream(), DefaultConfiguration.class);
    }

    @Bean(name = {"objectMapperBuilder"})
    public ObjectMapperBuilder createObjectMapperBuilder(
        ShardedScheduler schedulerService,
        Environment env,
        ElasticActorsNode node)
    {
        String basePackages = env.getProperty("ea.scan.packages",String.class,"");
        Boolean useAfterburner = env.getProperty("ea.base.useAfterburner",Boolean.class, FALSE);
        // @todo: fix version
        ObjectMapperBuilder builder =
                new ObjectMapperBuilder(node, schedulerService, basePackages, "1.0.0");
        builder.setUseAfterBurner(useAfterburner);
        return builder;
    }

    @Bean(name = "systemSerializationFramework")
    public SystemSerializationFramework createSystemSerializationFramework(SerializationFrameworks serializationFrameworks) {
        return new SystemSerializationFramework(serializationFrameworks);
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
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MicrometerTagCustomizer tagCustomizer)
    {
        int maximumSize = env.getProperty("ea.nodeCache.maximumSize",Integer.class,10240);
        long expirationCheckPeriod =
            env.getProperty("ea.nodeCache.expirationCheckPeriod", Long.class, 30000L);
        return new NodeActorCacheManager(
            maximumSize,
            expirationCheckPeriod,
            MicrometerConfiguration.build(env, meterRegistry, "nodeActorCache", tagCustomizer)
        );
    }

    @Bean(name = {"shardActorCacheManager"})
    public ShardActorCacheManager createShardActorCacheManager(
        Environment env,
        @Nullable @Qualifier("elasticActorsMeterRegistry") MeterRegistry meterRegistry,
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MicrometerTagCustomizer tagCustomizer)
    {
        int maximumSize = env.getProperty("ea.shardCache.maximumSize",Integer.class,10240);
        return new ShardActorCacheManager(
            maximumSize,
            MicrometerConfiguration.build(env, meterRegistry, "shardActorCache", tagCustomizer)
        );
    }

    @Bean(name = {"actorExecutor"}, destroyMethod = "shutdown")
    @DependsOn("asyncUpdateExecutor")
    public ThreadBoundExecutor createActorExecutor(
        Environment env,
        @Nullable @Qualifier("elasticActorsMeterRegistry") MeterRegistry meterRegistry,
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MicrometerTagCustomizer tagCustomizer)
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

    @Bean(name = {"internalActorSystem"}, destroyMethod = "shutdown")
    public InternalActorSystem createLocalActorSystemInstance(
        ElasticActorsNode node,
        @Qualifier("actorSystemConfiguration") InternalActorSystemConfiguration configuration,
        ManagedActorsRegistry managedActorsRegistry,
        NodeSelectorFactory nodeSelectorFactory)
    {
        return new LocalActorSystemInstance(
                node,
                node,
                configuration,
                nodeSelectorFactory,
                managedActorsRegistry);
    }

    @Bean(name = {"remoteActorSystems"})
    public RemoteActorSystems createRemoteActorSystems(
        ElasticActorsNode node,
        @Qualifier("actorSystemConfiguration") InternalActorSystemConfiguration configuration,
        @Qualifier("remoteActorSystemMessageQueueFactoryFactory")
            MessageQueueFactoryFactory remoteActorSystemMessageQueueFactoryFactory)
    {
        return new RemoteActorSystems(configuration, node, remoteActorSystemMessageQueueFactoryFactory);
    }

    @Bean(name = {"scheduler"})
    public ShardedScheduler createScheduler(
        Environment env,
        @Nullable @Qualifier("elasticActorsMeterRegistry") MeterRegistry meterRegistry,
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MicrometerTagCustomizer tagCustomizer)
    {
        int numberOfWorkers = env.getProperty("ea.shardedScheduler.workerCount", Integer.class, Runtime.getRuntime().availableProcessors());
        return new ShardedScheduler(
            numberOfWorkers,
            MicrometerConfiguration.build(env, meterRegistry, "scheduler", tagCustomizer)
        );
    }

    @Bean(name = {"actorSystemEventListenerService"})
    public ActorSystemEventListenerService createActorSystemEventListenerService(
        ActorSystemEventListenerRepository eventListenerRepository,
        InternalActorSystem actorSystem)
    {
        return new ActorSystemEventRegistryImpl(eventListenerRepository, actorSystem);
    }

    @Bean(name = {"internalActorSystemHealthCheck"})
    public InternalActorSystemHealthCheck createHealthCheck(InternalActorSystem internalActorSystem) {
        return new InternalActorSystemHealthCheck(internalActorSystem);
    }

    @Bean(name = {"actorStateUpdateProcessor"})
    public ActorStateUpdateProcessor createActorStateUpdateProcessor(
        Environment env,
        List<ActorStateUpdateListener> listeners,
        @Nullable @Qualifier("elasticActorsMeterRegistry") MeterRegistry meterRegistry,
        @Nullable @Qualifier("elasticActorsMeterTagCustomizer") MicrometerTagCustomizer tagCustomizer)
    {
        if(listeners.isEmpty()) {
            return new NoopActorStateUpdateProcessor();
        } else {
            return new DefaultActorStateUpdateProcessor(
                listeners,
                env,
                meterRegistry,
                tagCustomizer
            );
        }
    }

    @Bean(name = "nodeMetricsSettings")
    public MetricsSettings nodeMetricsSettings(Environment environment) {
        return MetricsSettings.build(environment, "node");
    }

    @Bean(name = "shardMetricsSettings")
    public MetricsSettings shardMetricsSettings(Environment environment) {
        return MetricsSettings.build(environment, "shard");
    }

    @Bean(name = "nodeLoggingSettings")
    public LoggingSettings nodeLoggingSettings(Environment environment) {
        return LoggingSettings.build(environment, "node");
    }

    @Bean(name = "shardLoggingSettings")
    public LoggingSettings shardLoggingSettings(Environment environment) {
        return LoggingSettings.build(environment, "shard");
    }

}
