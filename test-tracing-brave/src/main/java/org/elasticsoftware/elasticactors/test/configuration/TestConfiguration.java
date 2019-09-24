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

import brave.ErrorParser;
import brave.Tracing;
import brave.context.slf4j.MDCScopeDecorator;
import brave.propagation.B3Propagation;
import brave.propagation.CurrentTraceContext.ScopeDecorator;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.sampler.Sampler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.elasticsoftware.elasticactors.ActorSystemConfiguration;
import org.elasticsoftware.elasticactors.InternalActorSystemConfiguration;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.base.serialization.ObjectMapperBuilder;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerService;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.cluster.HashingNodeSelectorFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.cluster.LocalActorSystemInstance;
import org.elasticsoftware.elasticactors.cluster.NodeSelectorFactory;
import org.elasticsoftware.elasticactors.cluster.PhysicalNodeImpl;
import org.elasticsoftware.elasticactors.cluster.scheduler.SimpleScheduler;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.runtime.DefaultConfiguration;
import org.elasticsoftware.elasticactors.runtime.MessagesScanner;
import org.elasticsoftware.elasticactors.runtime.PluggableMessageHandlersScanner;
import org.elasticsoftware.elasticactors.serialization.SystemSerializationFramework;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateListener;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.DefaultActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.NoopActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.test.InternalActorSystemsImpl;
import org.elasticsoftware.elasticactors.test.cluster.NoopActorSystemEventRegistryService;
import org.elasticsoftware.elasticactors.test.cluster.SingleNodeClusterService;
import org.elasticsoftware.elasticactors.test.state.LoggingActorStateUpdateListener;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutorImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.aspectj.EnableSpringConfigured;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import zipkin2.Span;
import zipkin2.reporter.Reporter;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;

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
    private final NodeSelectorFactory nodeSelectorFactory = new HashingNodeSelectorFactory();
    private final PhysicalNode localNode = new PhysicalNodeImpl(UUIDTools.createRandomUUID().toString(), InetAddress.getLoopbackAddress(), true);

    @Bean(name = "systemInitializer")
    public SystemInitializer createSystemInitializer(LocalActorSystemInstance localActorSystemInstance, ClusterService clusterService) {
        return new SystemInitializer(localNode, localActorSystemInstance, clusterService);

    }

    @DependsOn("configuration") @Bean(name = {"internalActorSystem"})
    public LocalActorSystemInstance createLocalActorSystemInstance(InternalActorSystems internalActorSystems, InternalActorSystemConfiguration configuration) {
        return new LocalActorSystemInstance(localNode,internalActorSystems,configuration,nodeSelectorFactory);
    }

    @Bean(name = {"actorSystems,actorRefFactory"})
    public InternalActorSystemsImpl createInternalActorSystems(ApplicationContext applicationContext,
                                                           ClusterService clusterService) {
        return new InternalActorSystemsImpl(applicationContext, clusterService, localNode);
    }

    @Bean(name = {"configuration"})
    public ActorSystemConfiguration getConfiguration() throws IOException {
        // get the yaml resource
        Resource configResource = resourceLoader.getResource(env.getProperty("ea.node.config.location","classpath:ea-test.yaml"));
        // yaml mapper
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        return objectMapper.readValue(configResource.getInputStream(), DefaultConfiguration.class);
    }

    @Bean(name = {"objectMapper"})
    public ObjectMapper createObjectMapper(SimpleScheduler simpleScheduler, InternalActorSystemsImpl actorRefFactory) {
        // @todo: fix version
        return new ObjectMapperBuilder(actorRefFactory,simpleScheduler,"1.0.0").build();
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
    public ThreadBoundExecutor createActorExecutor() {
        int workers = Runtime.getRuntime().availableProcessors() * 3;
        return new ThreadBoundExecutorImpl(new DaemonThreadFactory("ACTOR-WORKER"),workers);
    }

    @Bean(name = {"queueExecutor"}, destroyMethod = "shutdown")
    public ThreadBoundExecutor createQueueExecutor() {
        int workers = Runtime.getRuntime().availableProcessors() * 3;
        return new ThreadBoundExecutorImpl(new DaemonThreadFactory("QUEUE-WORKER"),workers);
    }

    @Bean(name = {"scheduler"})
    public SimpleScheduler createScheduler() {
        return new SimpleScheduler();
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
    public ActorStateUpdateProcessor createActorStateUpdateProcessor(ApplicationContext applicationContext) {
        Map<String, ActorStateUpdateListener> listeners = applicationContext.getBeansOfType(ActorStateUpdateListener.class);
        if(listeners.isEmpty()) {
            return new NoopActorStateUpdateProcessor();
        } else {
            return new DefaultActorStateUpdateProcessor(listeners.values(), 1, 20);
        }
    }

    @Bean(name = {"loggingActorStateUpdateListener"})
    public LoggingActorStateUpdateListener createLoggingActorStateUpdateListener() {
        return new LoggingActorStateUpdateListener();
    }

    @Bean(name = "systemSerializationFramework")
    public SystemSerializationFramework createSystemSerializationFramework(InternalActorSystems internalActorSystems) {
        return new SystemSerializationFramework(internalActorSystems);
    }

    private static final ErrorParser PRINTER = new ErrorParser() {
        @Override protected void error(Throwable error, Object customizer) {
            System.out.println(String.format(
                    "Thread [%s]: Error reported, class=%s, message=%s",
                    Thread.currentThread().getName(),
                    error.getClass().getName(),
                    error.getMessage()));
        }
    };
    private static final Reporter<Span> SPAN_REPORTER = span ->
            System.out.println(String.format(
                    "Thread [%s]: Span reported, name=%s, spanId=%s, parentId=%s, traceId=%s, tags=%s",
                    Thread.currentThread().getName(),
                    span.name(),
                    span.id(),
                    span.parentId(),
                    span.traceId(),
                    toString(span.tags())));
    private static final ScopeDecorator DECORATOR = (currentSpan, scope) -> {
        System.out.println(String.format(
                "Thread [%s]: Decorated scope, spanId=%s, parentId=%s, traceId=%s",
                Thread.currentThread().getName(),
                currentSpan.spanIdString(),
                currentSpan.parentIdString(),
                currentSpan.traceIdString()));
        return scope;
    };

    @Bean
    public Tracing tracing() {
        return Tracing.newBuilder()
                .currentTraceContext(ThreadLocalCurrentTraceContext.newBuilder()
                        .addScopeDecorator(MDCScopeDecorator.create())
                        .addScopeDecorator(DECORATOR)
                        .build())
                .sampler(Sampler.ALWAYS_SAMPLE)
                .spanReporter(SPAN_REPORTER)
                .errorParser(PRINTER)
                .propagationFactory(B3Propagation.FACTORY)
                .build();
    }

    private static String toString(Map<?, ?> map) {
        if (map != null) {
            StringJoiner joiner = new StringJoiner(",", "{", "}");
            for (Entry<?, ?> entry : map.entrySet()) {
                joiner.add("{\"" + entry.getKey() + "\":\"" + entry.getValue() + "\"}");
            }
            return joiner.toString();
        }
        return "null";
    }

}