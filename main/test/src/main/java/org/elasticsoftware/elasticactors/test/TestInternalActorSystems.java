/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.ActorRefTools;
import org.elasticsoftware.elasticactors.cluster.ActorShardRef;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.cluster.LocalClusterActorNodeRef;
import org.elasticsoftware.elasticactors.cluster.RebalancingEventListener;
import org.elasticsoftware.elasticactors.cluster.ServiceActorRef;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.MessagingSystemDeserializers;
import org.elasticsoftware.elasticactors.serialization.MessagingSystemSerializers;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SystemDeserializers;
import org.elasticsoftware.elasticactors.serialization.SystemSerializers;
import org.elasticsoftware.elasticactors.util.ManifestTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Joost van de Wijgerd
 */
public final class TestInternalActorSystems implements InternalActorSystems, ActorRefFactory {

    private final static Logger logger = LoggerFactory.getLogger(TestInternalActorSystems.class);

    private final SystemSerializers systemSerializers = new MessagingSystemSerializers(this);
    private final SystemDeserializers systemDeserializers = new MessagingSystemDeserializers(this,this);
    private final Map<Class<? extends SerializationFramework>,SerializationFramework> serializationFrameworks = new ConcurrentHashMap<>();
    private final ApplicationContext applicationContext;
    private final ClusterService clusterService;
    private final PhysicalNode localNode;
    private final ActorRefTools actorRefTools;
    private InternalActorSystem internalActorSystem;

    public TestInternalActorSystems(ApplicationContext applicationContext,
                                    ClusterService clusterService,
                                    PhysicalNode localNode) {
        this.applicationContext = applicationContext;
        this.clusterService = clusterService;
        this.localNode = localNode;
        this.actorRefTools = new ActorRefTools(this);
    }

    @PreDestroy
    public void destroy() {

    }

    @Override
    public ActorRef create(String refSpec) {
        return actorRefTools.parse(refSpec);
    }

    @Override
    public ActorRef createPersistentActorRef(ActorShard shard, String actorId) {
        return new ActorShardRef(getClusterName(), shard, actorId, get(null));
    }

    @Override
    public ActorRef createTempActorRef(ActorNode node, String actorId) {
        return new LocalClusterActorNodeRef(get(null), getClusterName(), node, actorId);
    }

    @Override
    public ActorRef createServiceActorRef(ActorNode node, String actorId) {
        return new ServiceActorRef(get(null), getClusterName(), node, actorId);
    }

    @Override
    public String getActorStateVersion(Class<? extends ElasticActor> actorClass) {
        return ManifestTools.extractActorStateVersion(actorClass);
    }

    @Override
    public String getClusterName() {
        return "testcluster";
    }

    @Override
    public InternalActorSystem get(String name) {
        if (internalActorSystem == null) {
            internalActorSystem = applicationContext.getBean(InternalActorSystem.class);
        }
        return internalActorSystem;
    }

    @Override
    public ActorSystem getRemote(String clusterName, String actorSystemName) {
        return null; // not supported yet
    }

    @Override
    public ActorSystem getRemote(String actorSystemName) {
        return null;
    }

    @Override
    public void registerRebalancingEventListener(RebalancingEventListener eventListener) {
        // not supported (silently ignore)
    }

    @Override
    public <T> MessageSerializer<T> getSystemMessageSerializer(Class<T> messageClass) {
        return systemSerializers.get(messageClass);
    }

    @Override
    public <T> MessageDeserializer<T> getSystemMessageDeserializer(Class<T> messageClass) {
        return systemDeserializers.get(messageClass);
    }

    @Override
    public SerializationFramework getSerializationFramework(Class<? extends SerializationFramework> frameworkClass) {
        return serializationFrameworks.computeIfAbsent(frameworkClass, applicationContext::getBean);
    }
}
