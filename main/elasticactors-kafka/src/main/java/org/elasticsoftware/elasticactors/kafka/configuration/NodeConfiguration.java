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

package org.elasticsoftware.elasticactors.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.elasticsoftware.elasticactors.ActorLifecycleListenerRegistry;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.InternalActorSystemConfiguration;
import org.elasticsoftware.elasticactors.ManagedActorsRegistry;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.base.serialization.ObjectMapperBuilder;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.cache.ShardActorCacheManager;
import org.elasticsoftware.elasticactors.cluster.HashingNodeSelectorFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.NodeSelectorFactory;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRefFactory;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRefTools;
import org.elasticsoftware.elasticactors.health.InternalActorSystemHealthCheck;
import org.elasticsoftware.elasticactors.kafka.KafkaActorSystemInstance;
import org.elasticsoftware.elasticactors.kafka.serialization.CompressingSerializer;
import org.elasticsoftware.elasticactors.kafka.serialization.DecompressingDeserializer;
import org.elasticsoftware.elasticactors.kafka.state.PersistentActorStoreFactory;
import org.elasticsoftware.elasticactors.runtime.DefaultConfiguration;
import org.elasticsoftware.elasticactors.runtime.ElasticActorsNode;
import org.elasticsoftware.elasticactors.runtime.ManagedActorsScanner;
import org.elasticsoftware.elasticactors.runtime.MessagesScanner;
import org.elasticsoftware.elasticactors.runtime.PluggableMessageHandlersScanner;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.serialization.SystemSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.internal.PersistentActorDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.PersistentActorSerializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScans;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;

@ComponentScans({
    @ComponentScan("org.elasticsoftware.elasticactors.tracing.spring"),
    @ComponentScan("org.elasticsoftware.elasticactors.metrics.spring")
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
    public Cache<String, ActorRef> createActorRefCache(Environment env) {
        int maximumSize = env.getProperty("ea.actorRefCache.maximumSize", Integer.class, 10240);
        return CacheBuilder.newBuilder().maximumSize(maximumSize).build();
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
    public ObjectMapperBuilder createObjectMapperBuilder(Environment env, ElasticActorsNode node) {
        String basePackages = env.getProperty("ea.scan.packages",String.class,"");
        Boolean useAfterburner = env.getProperty("ea.base.useAfterburner",Boolean.class,Boolean.FALSE);
        ScheduledMessageRefFactory scheduledMessageRefFactory = refSpec -> ScheduledMessageRefTools.parse(refSpec, node);
        // @todo: fix version
        ObjectMapperBuilder builder =
                new ObjectMapperBuilder(node, scheduledMessageRefFactory, basePackages, "1.0.0");
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

    @Bean(name = {"nodeSelectorFactory"})
    public NodeSelectorFactory getNodeSelectorFactory() {
        return new HashingNodeSelectorFactory();
    }

    @Bean(name = {"nodeActorCacheManager"})
    public NodeActorCacheManager createNodeActorCacheManager(Environment env) {
        int maximumSize = env.getProperty("ea.nodeCache.maximumSize",Integer.class,10240);
        return new NodeActorCacheManager(maximumSize);
    }

    @Bean(name = {"shardActorCacheManager"})
    public ShardActorCacheManager createShardActorCacheManager(Environment env) {
        int maximumSize = env.getProperty("ea.shardCache.maximumSize",Integer.class,10240);
        return new ShardActorCacheManager(maximumSize);
    }

    @Bean(name = {"internalActorSystem"})
    public InternalActorSystem createLocalActorSystemInstance(
        ElasticActorsNode node,
        Environment env,
        ShardActorCacheManager shardActorCacheManager,
        NodeActorCacheManager nodeActorCacheManager,
        ActorLifecycleListenerRegistry actorLifecycleListenerRegistry,
        PersistentActorStoreFactory persistentActorStoreFactory,
        ManagedActorsRegistry managedActorsRegistry,
        @Qualifier("actorSystemConfiguration") InternalActorSystemConfiguration configuration,
        NodeSelectorFactory nodeSelectorFactory,
        @Qualifier("actorRefCache") Cache<String, ActorRef> actorRefCache)
    {
        final int workers = env.getProperty("ea.shardThreads.workerCount",Integer.class,
                Runtime.getRuntime().availableProcessors());
        final String bootstrapServers = env.getRequiredProperty("ea.kafka.bootstrapServers");
        final Integer compressionThreshold = env.getProperty("ea.persistentActorRepository.compressionThreshold",Integer.class, 512);
        Serializer<PersistentActor<ShardKey>,byte[]> serializer = new CompressingSerializer<>(new PersistentActorSerializer(node),compressionThreshold);
        Deserializer<byte[],PersistentActor<ShardKey>> deserializer = new DecompressingDeserializer<>(new PersistentActorDeserializer(node, node));
        // NOTE: the node topic will be created with ea.shardThreads.workerCount number of partitions, changing this
        // value will require you to update the topic or face serious issues otherwise
        return new KafkaActorSystemInstance(
                node,
                configuration,
                nodeSelectorFactory,
                workers,
                bootstrapServers,
                actorRefCache,
                shardActorCacheManager,
                nodeActorCacheManager,
                serializer,
                deserializer,
                actorLifecycleListenerRegistry,
                persistentActorStoreFactory,
                managedActorsRegistry);
    }

    @Bean(name = {"internalActorSystemHealthCheck"})
    public InternalActorSystemHealthCheck createHealthCheck(InternalActorSystem internalActorSystem) {
        return new InternalActorSystemHealthCheck(internalActorSystem);
    }

    @Bean(name = {"persistentActorStoreFactory"})
    public PersistentActorStoreFactory createPersistentActorStoreFactory(Environment env) throws Exception {
        String className = env.getProperty("ea.kafka.persistentActorStore.factoryClass", String.class, "InMemoryPeristentActorStoreFactory");
        // if it is a simple classname then we need to append the default package
        if(!className.contains(".")) {
            return (PersistentActorStoreFactory) getClassHelper().forName(
                "org.elasticsoftware.elasticactors.kafka.state." + className).newInstance();
        } else {
            return (PersistentActorStoreFactory) getClassHelper().forName(className).newInstance();
        }
    }
}
