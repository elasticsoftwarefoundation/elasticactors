/*
 * Copyright 2013 - 2017 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.elasticsoftware.elasticactors.InternalActorSystemConfiguration;
import org.elasticsoftware.elasticactors.base.serialization.ObjectMapperBuilder;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.*;
import org.elasticsoftware.elasticactors.cluster.scheduler.ShardedScheduler;
import org.elasticsoftware.elasticactors.health.InternalActorSystemHealthCheck;
import org.elasticsoftware.elasticactors.runtime.DefaultConfiguration;
import org.elasticsoftware.elasticactors.runtime.ElasticActorsNode;
import org.elasticsoftware.elasticactors.runtime.MessagesScanner;
import org.elasticsoftware.elasticactors.runtime.PluggableMessageHandlersScanner;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutorImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.InetAddress;

/**
 * @author Joost van de Wijgerd
 */

public class NodeConfiguration {
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
        node = new ElasticActorsNode(clusterName, nodeId, nodeAddress, configuration);
    }


    @Bean(name = {"elasticActorsNode,actorSystems,actorRefFactory"})
    public ElasticActorsNode getNode() {
        return node;
    }

    @Bean
    public InternalActorSystemConfiguration getConfiguration() {
        return configuration;
    }

    @Bean(name = {"objectMapper"})
    public ObjectMapper createObjectMapper(ShardedScheduler schedulerService) {
        String basePackages = env.getProperty("ea.scan.packages",String.class,"");
        Boolean useAfterburner = env.getProperty("ea.base.useAfterburner",Boolean.class,Boolean.FALSE);
        // @todo: fix version
        ObjectMapperBuilder builder = new ObjectMapperBuilder(node,schedulerService,"1.0.0",basePackages);
        builder.setUseAfterBurner(useAfterburner);
        return builder.build();
    }

    @Bean(name = {"messagesScanner"})
    public MessagesScanner createMessageScanner() {
        return new MessagesScanner();
    }

    @Bean(name = {"messageHandlersRegistry"})
    public PluggableMessageHandlersScanner createPluggableMessagesHandlersScanner() {
        return new PluggableMessageHandlersScanner();
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
        final Boolean useDisruptor = env.getProperty("ea.actorExecutor.useDisruptor",Boolean.class,Boolean.FALSE);
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
        final Boolean useDisruptor = env.getProperty("ea.actorExecutor.useDisruptor",Boolean.class,Boolean.FALSE);
        if(useDisruptor) {
            return new org.elasticsoftware.elasticactors.util.concurrent.disruptor.ThreadBoundExecutorImpl(new DaemonThreadFactory("QUEUE-WORKER"), workers);
        } else {
            return new ThreadBoundExecutorImpl(new DaemonThreadFactory("QUEUE-WORKER"), workers);
        }
    }

    @Bean(name = {"internalActorSystem"}, destroyMethod = "shutdown")
    public InternalActorSystem createLocalActorSystemInstance() {
        return new LocalActorSystemInstance(node,node,configuration,nodeSelectorFactory);
    }

    @Bean(name = {"remoteActorSystems"})
    public RemoteActorSystems createRemoteActorSystems() {
        return new RemoteActorSystems(configuration,node);
    }

    @Bean(name = {"scheduler"})
    public ShardedScheduler createScheduler() {
        //@todo: maybe configure scheduler here with number of workers
        return new ShardedScheduler();
    }

    @Bean(name = {"actorSystemEventListenerService"})
    public ActorSystemEventListenerService createActorSystemEventListenerService() {
        return new ActorSystemEventRegistryImpl();
    }

    @Bean(name = {"internalActorSystemHealthCheck"})
    public InternalActorSystemHealthCheck createHealthCheck(InternalActorSystem internalActorSystem) {
        return new InternalActorSystemHealthCheck(internalActorSystem);
    }
}
