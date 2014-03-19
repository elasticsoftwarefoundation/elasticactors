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

package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.*;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.internal.SystemDeserializers;
import org.elasticsoftware.elasticactors.serialization.internal.SystemSerializers;
import org.elasticsoftware.elasticactors.test.cluster.SingleNodeClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Arrays;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class InternalActorSystemsImpl implements InternalActorSystems, ActorRefFactory {
    private final SystemSerializers systemSerializers = new SystemSerializers(this);
    private final SystemDeserializers systemDeserializers = new SystemDeserializers(this,this);
    @Autowired
    private ApplicationContext applicationContext;
    @Autowired
    private LocalActorSystemInstance localActorSystemInstance;
    @Autowired
    private ClusterService clusterService;
    private final PhysicalNode localNode;

    public InternalActorSystemsImpl(PhysicalNode localNode) {
        this.localNode = localNode;
    }

    @PostConstruct
    public void initialize() throws Exception {
        final List<PhysicalNode> localNodes = Arrays.<PhysicalNode>asList(localNode);
        localActorSystemInstance.updateNodes(localNodes);
        localActorSystemInstance.distributeShards(localNodes);
        // signal master elected
        clusterService.reportReady();
    }

    @PreDestroy
    public void destroy() {

    }

    @Override
    public ActorRef create(String refSpec) {
        return ActorRefTools.parse(refSpec, this);
    }

    @Override
    public ActorRef createPersistentActorRef(ActorShard shard, String actorId) {
        return new ActorShardRef(getClusterName(),shard,actorId);
    }

    @Override
    public ActorRef createTempActorRef(ActorNode node, String actorId) {
        return new LocalClusterActorNodeRef(getClusterName(),node,actorId);
    }

    @Override
    public ActorRef createServiceActorRef(ActorNode node, String actorId) {
        return new ServiceActorRef(getClusterName(),node,actorId);
    }

    @Override
    public String getClusterName() {
        return "testcluster";
    }

    @Override
    public InternalActorSystem get(String name) {
        return applicationContext.getBean(InternalActorSystem.class);
    }

    @Override
    public ActorSystem getRemote(String clusterName, String actorSystemName) {
        return null; // not supported yet
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
        return applicationContext.getBean(frameworkClass);
    }
}
