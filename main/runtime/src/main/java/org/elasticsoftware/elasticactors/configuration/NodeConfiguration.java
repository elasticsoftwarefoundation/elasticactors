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

package org.elasticsoftware.elasticactors.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.InternalActorSystemConfiguration;
import org.elasticsoftware.elasticactors.ManagedActorsRegistry;
import org.elasticsoftware.elasticactors.base.serialization.ObjectMapperBuilder;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerService;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventRegistryImpl;
import org.elasticsoftware.elasticactors.cluster.HashingNodeSelectorFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.LocalActorSystemInstance;
import org.elasticsoftware.elasticactors.cluster.NodeSelectorFactory;
import org.elasticsoftware.elasticactors.cluster.RemoteActorSystems;
import org.elasticsoftware.elasticactors.cluster.logging.LoggingSettings;
import org.elasticsoftware.elasticactors.cluster.metrics.MetricsSettings;
import org.elasticsoftware.elasticactors.cluster.scheduler.ShardedScheduler;
import org.elasticsoftware.elasticactors.health.InternalActorSystemHealthCheck;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactoryFactory;
import org.elasticsoftware.elasticactors.runtime.DefaultConfiguration;
import org.elasticsoftware.elasticactors.runtime.ElasticActorsNode;
import org.elasticsoftware.elasticactors.runtime.ManagedActorsScanner;
import org.elasticsoftware.elasticactors.runtime.MessagesScanner;
import org.elasticsoftware.elasticactors.runtime.PluggableMessageHandlersScanner;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;
import org.elasticsoftware.elasticactors.serialization.SystemSerializationFramework;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateListener;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.DefaultActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.NoopActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutorImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;

import static java.lang.Boolean.FALSE;

/**
 * @author Joost van de Wijgerd
 */

public class NodeConfiguration {

    private static final String EA_METRICS_OVERRIDES = "ea.metrics.messages.overrides.";

    @Autowired
    private Environment env;
    @Autowired
    private ResourceLoader resourceLoader;

    private final NodeSelectorFactory nodeSelectorFactory = new HashingNodeSelectorFactory();
    private ElasticActorsNode node;
    private InternalActorSystemConfiguration configuration;

    @PostConstruct
    public void init() throws IOException {
        // get the yaml resource
        Resource configResource = resourceLoader.getResource(env.getProperty("ea.node.config.location","classpath:ea-default.yaml"));
        // yaml mapper
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        configuration = objectMapper.readValue(configResource.getInputStream(), DefaultConfiguration.class);
        String nodeId = env.getRequiredProperty("ea.node.id");
        InetAddress nodeAddress = InetAddress.getByName(env.getRequiredProperty("ea.node.address"));
        String clusterName = env.getRequiredProperty("ea.cluster");
        int maximumSize = env.getProperty("ea.actorRefCache.maximumSize",Integer.class,10240);
        Cache<String,ActorRef> actorRefCache = CacheBuilder.newBuilder().maximumSize(maximumSize).build();
        node = new ElasticActorsNode(clusterName, nodeId, nodeAddress, actorRefCache);
    }


    @Bean(name = {
            "elasticActorsNode",
            "actorSystems",
            "actorRefFactory",
            "serializationFrameworks"
    })
    public ElasticActorsNode getNode() {
        return node;
    }

    @Bean
    public InternalActorSystemConfiguration getConfiguration() {
        return configuration;
    }

    @Bean(name = {"objectMapperBuilder"})
    public ObjectMapperBuilder createObjectMapperBuilder(ShardedScheduler schedulerService) {
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

    @Bean(name = {"managedActorsScanner"})
    public ManagedActorsScanner createManagedActorsScanner() {
        return new ManagedActorsScanner();
    }

    @Bean(name = {"messagesScanner"})
    public MessagesScanner createMessageScanner() {
        return new MessagesScanner();
    }

    @Bean(name = {"messageHandlersRegistry"})
    public PluggableMessageHandlersScanner createPluggableMessagesHandlersScanner(ApplicationContext applicationContext) {
        return new PluggableMessageHandlersScanner(applicationContext);
    }

    @Bean(name = {"nodeSelectorFactory"})
    public NodeSelectorFactory getNodeSelectorFactory() {
        return nodeSelectorFactory;
    }

    @Bean(name = {"nodeActorCacheManager"})
    public NodeActorCacheManager createNodeActorCacheManager() {
        int maximumSize = env.getProperty("ea.nodeCache.maximumSize",Integer.class,10240);
        return new NodeActorCacheManager(maximumSize);
    }

    @Bean(name = {"shardActorCacheManager"})
    public ShardActorCacheManager createShardActorCacheManager() {
        int maximumSize = env.getProperty("ea.shardCache.maximumSize",Integer.class,10240);
        return new ShardActorCacheManager(maximumSize);
    }

    @Bean(name = {"actorExecutor"}, destroyMethod = "shutdown")
    @DependsOn("asyncUpdateExecutor")
    public ThreadBoundExecutor createActorExecutor() {
        final int workers = env.getProperty("ea.actorExecutor.workerCount",Integer.class,Runtime.getRuntime().availableProcessors() * 3);
        final Boolean useDisruptor = env.getProperty("ea.actorExecutor.useDisruptor",Boolean.class, FALSE);
        if(useDisruptor) {
            return new org.elasticsoftware.elasticactors.util.concurrent.disruptor.ThreadBoundExecutorImpl(new DaemonThreadFactory("ACTOR-WORKER"),workers);
        } else {
            return new ThreadBoundExecutorImpl(new DaemonThreadFactory("ACTOR-WORKER"), workers);
        }
    }

    @Bean(name = {"queueExecutor"}, destroyMethod = "shutdown")
    @DependsOn("actorExecutor")
    public ThreadBoundExecutor createQueueExecutor() {
        final int workers = env.getProperty("ea.queueExecutor.workerCount",Integer.class,Runtime.getRuntime().availableProcessors() * 3);
        final Boolean useDisruptor = env.getProperty("ea.actorExecutor.useDisruptor",Boolean.class, FALSE);
        if(useDisruptor) {
            return new org.elasticsoftware.elasticactors.util.concurrent.disruptor.ThreadBoundExecutorImpl(new DaemonThreadFactory("QUEUE-WORKER"), workers);
        } else {
            return new ThreadBoundExecutorImpl(new DaemonThreadFactory("QUEUE-WORKER"), workers);
        }
    }

    @Bean(name = {"internalActorSystem"}, destroyMethod = "shutdown")
    public InternalActorSystem createLocalActorSystemInstance(
                ManagedActorsRegistry managedActorsRegistry) {
        return new LocalActorSystemInstance(
                node,
                node,
                configuration,
                nodeSelectorFactory,
                managedActorsRegistry);
    }

    @Bean(name = {"remoteActorSystems"})
    public RemoteActorSystems createRemoteActorSystems(@Qualifier("remoteActorSystemMessageQueueFactoryFactory") MessageQueueFactoryFactory remoteActorSystemMessageQueueFactoryFactory) {
        return new RemoteActorSystems(configuration, node, remoteActorSystemMessageQueueFactoryFactory);
    }

    @Bean(name = {"scheduler"})
    public ShardedScheduler createScheduler() {
        int numberOfWorkers = env.getProperty("ea.shardedScheduler.workerCount", Integer.class, Runtime.getRuntime().availableProcessors());
        return new ShardedScheduler(numberOfWorkers);
    }

    @Bean(name = {"actorSystemEventListenerService"})
    public ActorSystemEventListenerService createActorSystemEventListenerService() {
        return new ActorSystemEventRegistryImpl();
    }

    @Bean(name = {"internalActorSystemHealthCheck"})
    public InternalActorSystemHealthCheck createHealthCheck(InternalActorSystem internalActorSystem) {
        return new InternalActorSystemHealthCheck(internalActorSystem);
    }

    @Bean(name = {"actorStateUpdateProcessor"})
    public ActorStateUpdateProcessor createActorStateUpdateProcessor(ApplicationContext applicationContext) {
        Map<String, ActorStateUpdateListener> listeners = applicationContext.getBeansOfType(ActorStateUpdateListener.class);
        if(listeners.isEmpty()) {
            return new NoopActorStateUpdateProcessor();
        } else {
            final int workers = env.getProperty("ea.actorStateUpdateProcessor.workerCount",Integer.class,1);
            final int maxBatchSize = env.getProperty("ea.actorStateUpdateProcessor.maxBatchSize",Integer.class,20);
            return new DefaultActorStateUpdateProcessor(listeners.values(), workers, maxBatchSize);
        }
    }

    @Bean(name = "nodeMetricsSettings")
    public MetricsSettings nodeMetricsSettings(Environment environment) {
        boolean metricsEnabled =
            environment.getProperty("ea.metrics.node.messaging.enabled", Boolean.class, false);
        Long messageDeliveryWarnThreshold =
            environment.getProperty("ea.metrics.node.messaging.delivery.warn.threshold", Long.class);
        Long messageHandlingWarnThreshold =
            environment.getProperty("ea.metrics.node.messaging.handling.warn.threshold", Long.class);

        return new MetricsSettings(
            metricsEnabled,
            messageDeliveryWarnThreshold,
            messageHandlingWarnThreshold,
            null
        );
    }

    @Bean(name = "shardMetricsSettings")
    public MetricsSettings shardMetricsSettings(Environment environment) {
        boolean metricsEnabled =
            environment.getProperty("ea.metrics.shard.messaging.enabled", Boolean.class, false);
        Long messageDeliveryWarnThreshold =
            environment.getProperty("ea.metrics.shard.messaging.delivery.warn.threshold", Long.class);
        Long messageHandlingWarnThreshold =
            environment.getProperty("ea.metrics.shard.messaging.handling.warn.threshold", Long.class);
        Long serializationWarnThreshold =
            environment.getProperty("ea.metrics.shard.serialization.warn.threshold", Long.class);

        return new MetricsSettings(
            metricsEnabled,
            messageDeliveryWarnThreshold,
            messageHandlingWarnThreshold,
            serializationWarnThreshold
        );
    }

    @Bean(name = "nodeLoggingSettings")
    public LoggingSettings nodeLoggingSettings(Environment environment) {
        boolean loggingEnabled =
            environment.getProperty("ea.logging.node.messaging.enabled", Boolean.class, false);

        return new LoggingSettings(
            loggingEnabled,
            buildOverridesMap(environment)
        );
    }

    @Bean(name = "shardLoggingSettings")
    public LoggingSettings shardLoggingSettings(Environment environment) {
        boolean loggingEnabled =
            environment.getProperty("ea.logging.shard.messaging.enabled", Boolean.class, false);

        return new LoggingSettings(
            loggingEnabled,
            buildOverridesMap(environment)
        );
    }

    private ImmutableMap<String, Message.LogFeature[]> buildOverridesMap(Environment environment) {
        ImmutableMap.Builder<String, Message.LogFeature[]> mapBuilder = ImmutableMap.builder();
        if (environment instanceof ConfigurableEnvironment) {
            for (PropertySource<?> propertySource : ((ConfigurableEnvironment) environment).getPropertySources()) {
                if (propertySource instanceof EnumerablePropertySource) {
                    for (String key : ((EnumerablePropertySource<?>) propertySource).getPropertyNames()) {
                        if (key.length() > EA_METRICS_OVERRIDES.length() && key.startsWith(EA_METRICS_OVERRIDES)) {
                            Object property = propertySource.getProperty(key);
                            if (property != null) {
                                String value = property.toString();
                                Message.LogFeature[] features = Arrays.stream(value.split(","))
                                    .map(String::trim)
                                    .filter(s -> !s.isEmpty())
                                    .map(String::toUpperCase)
                                    .map(Message.LogFeature::valueOf)
                                    .distinct()
                                    .toArray(Message.LogFeature[]::new);
                                String className = key.substring(EA_METRICS_OVERRIDES.length());
                                mapBuilder.put(className, features);
                            }
                        }
                    }
                }
            }
        }
        return mapBuilder.build();
    }
}
