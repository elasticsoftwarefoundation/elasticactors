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

package org.elasticsoftware.elasticactors.kafka.cluster;

import com.google.common.cache.Cache;
import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.ActorShardRef;
import org.elasticsoftware.elasticactors.cluster.BaseDisconnectedActorRef;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.cluster.RebalancingEventListener;
import org.elasticsoftware.elasticactors.cluster.ServiceActorRef;
import org.elasticsoftware.elasticactors.kafka.KafkaActorNode;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.MessagingSystemDeserializers;
import org.elasticsoftware.elasticactors.serialization.MessagingSystemSerializers;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SystemDeserializers;
import org.elasticsoftware.elasticactors.serialization.SystemSerializers;

import javax.annotation.Nullable;

public final class KafkaInternalActorSystems implements InternalActorSystems, ActorRefFactory {
    private final InternalActorSystems delegate;
    private final Cache<String,ActorRef> actorRefCache;
    private final KafkaActorRefTools actorRefTools;
    private final SystemSerializers systemSerializers = new MessagingSystemSerializers(this);
    private final SystemDeserializers systemDeserializers = new MessagingSystemDeserializers(this, this);

    public KafkaInternalActorSystems(InternalActorSystems delegate, Cache<String, ActorRef> actorRefCache) {
        this.delegate = delegate;
        this.actorRefCache = actorRefCache;
        this.actorRefTools = new KafkaActorRefTools(this);
    }

    @Override
    public String getClusterName() {
        return delegate.getClusterName();
    }

    @Override
    public InternalActorSystem get(String name) {
        return delegate.get(name);
    }

    @Override
    public ActorSystem getRemote(String clusterName, @Nullable String actorSystemName) {
        throw new UnsupportedOperationException("Remote ActorSystems are currently not supported for Kafka based implementation");
    }

    @Override
    public ActorSystem getRemote(String actorSystemName) {
        throw new UnsupportedOperationException("Remote ActorSystems are currently not supported for Kafka based implementation");
    }

    @Override
    public void registerRebalancingEventListener(RebalancingEventListener eventListener) {
        delegate.registerRebalancingEventListener(eventListener);
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
        return delegate.getSerializationFramework(frameworkClass);
    }

    @Override
    public String getActorStateVersion(Class<? extends ElasticActor> actorClass) {
        return delegate.getActorStateVersion(actorClass);
    }

    @Override
    public ActorRef createPersistentActorRef(ActorShard shard, String actorId) {
        final String refSpec = ActorShardRef.generateRefSpec(delegate.getClusterName(),shard,actorId);
        ActorRef ref = actorRefCache.getIfPresent(refSpec);
        if(ref == null) {
            ref = new ActorShardRef(delegate.getClusterName(), shard, actorId, get(null));
            actorRefCache.put(refSpec,ref);
        }
        return ref;
    }

    @Override
    public ActorRef createTempActorRef(ActorNode node, String actorId) {
        throw new UnsupportedOperationException("Not supported in the KafkaActorSystem implementation as the node partition cannot be determined");
    }

    public ActorRef createTempActorRef(KafkaActorNode node, int partition, String actorId) {
        final String refSpec = LocalClusterPartitionedActorNodeRef.generateRefSpec(delegate.getClusterName(), node, partition, actorId);
        ActorRef ref = actorRefCache.getIfPresent(refSpec);
        if(ref == null) {
            ref = new LocalClusterPartitionedActorNodeRef(get(null), delegate.getClusterName(), node, partition, actorId);
            actorRefCache.put(refSpec,ref);
        }
        return ref;
    }

    @Override
    public ActorRef createServiceActorRef(ActorNode node, String actorId) {
        final String refSpec = ServiceActorRef.generateRefSpec(delegate.getClusterName() ,node,actorId);
        ActorRef ref = actorRefCache.getIfPresent(refSpec);
        if(ref == null) {
            ref = new ServiceActorRef(get(null), delegate.getClusterName(), node, actorId);
            actorRefCache.put(refSpec,ref);
        }
        return ref;
    }

    @Override
    public ActorRef create(String refSpec) {
        ActorRef actorRef = actorRefCache.getIfPresent(refSpec);
        if(actorRef == null) {
            actorRef = actorRefTools.parse(refSpec);
            if (!(actorRef instanceof BaseDisconnectedActorRef)) {
                actorRefCache.put(refSpec, actorRef);
            }
        }
        return actorRef;
    }
}
