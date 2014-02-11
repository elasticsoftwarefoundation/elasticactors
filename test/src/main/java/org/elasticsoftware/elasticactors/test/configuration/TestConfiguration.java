/*
 * Copyright 2013 - 2014 The Original Authors
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

package org.elasticsoftware.elasticactors.test.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.elasticsoftware.elasticactors.ActorSystemConfiguration;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.base.serialization.ObjectMapperBuilder;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.*;
import org.elasticsoftware.elasticactors.cluster.scheduler.SchedulerService;
import org.elasticsoftware.elasticactors.cluster.scheduler.SimpleScheduler;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.runtime.DefaultConfiguration;
import org.elasticsoftware.elasticactors.runtime.MessagesScanner;
import org.elasticsoftware.elasticactors.test.InternalActorSystemsImpl;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutorImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.aspectj.EnableSpringConfigured;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.InetAddress;

/**
 * @author Joost van de Wijgerd
 */
@Configuration
@EnableSpringConfigured
@Import({BackplaneConfiguration.class, MessagingConfiguration.class})
public class TestConfiguration {
    @Autowired
    private Environment env;
    @Autowired
    private ResourceLoader resourceLoader;
    private ActorSystemConfiguration configuration;
    private final NodeSelectorFactory nodeSelectorFactory = new HashingNodeSelectorFactory();
    private final PhysicalNode localNode = new PhysicalNodeImpl(UUIDTools.createRandomUUID().toString(), InetAddress.getLoopbackAddress(), true);
    private final InternalActorSystemsImpl internalActorSystems = new InternalActorSystemsImpl(localNode);

    @PostConstruct
    public void init() throws IOException {
        // get the yaml resource
        Resource configResource = resourceLoader.getResource(env.getProperty("ea.node.config.location","classpath:ea-test.yaml"));
        // yaml mapper
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        configuration = objectMapper.readValue(configResource.getInputStream(), DefaultConfiguration.class);
    }

    @DependsOn("configuration") @Bean(name = {"internalActorSystem"})
    public InternalActorSystem createLocalActorSystemInstance() {
        return new LocalActorSystemInstance(localNode,internalActorSystems,configuration,nodeSelectorFactory);
    }

    @DependsOn("internalActorSystem") @Bean(name = {"actorSystems,actorRefFactory"})
    public InternalActorSystemsImpl getActorSystemsImpl() {
        return internalActorSystems;
    }

    @Bean(name = {"configuration"})
    public ActorSystemConfiguration getConfiguration() {
        return configuration;
    }

    @Bean(name = {"objectMapper"})
    public ObjectMapper createObjectMapper() {
        // @todo: fix version
        return new ObjectMapperBuilder(internalActorSystems,"1.0.0").build();
    }

    @Bean(name = {"messagesScanner"})
    public MessagesScanner createMessageScanner() {
        return new MessagesScanner();
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
    public ThreadBoundExecutor<String> createActorExecutor() {
        int workers = Runtime.getRuntime().availableProcessors() * 3;
        return new ThreadBoundExecutorImpl(new DaemonThreadFactory("ACTOR-WORKER"),workers);
    }

    @Bean(name = {"queueExecutor"}, destroyMethod = "shutdown")
    public ThreadBoundExecutor<String> createQueueExecutor() {
        int workers = Runtime.getRuntime().availableProcessors() * 3;
        return new ThreadBoundExecutorImpl(new DaemonThreadFactory("QUEUE-WORKER"),workers);
    }



    @Bean(name = {"scheduler"})
    public SchedulerService createScheduler() {
        return new SimpleScheduler();
    }
}
