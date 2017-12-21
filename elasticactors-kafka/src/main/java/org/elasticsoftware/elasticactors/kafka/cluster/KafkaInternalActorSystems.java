package org.elasticsoftware.elasticactors.kafka.cluster;

import com.google.common.cache.Cache;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.*;
import org.elasticsoftware.elasticactors.kafka.KafkaActorNode;
import org.elasticsoftware.elasticactors.serialization.*;

import javax.annotation.Nullable;

public final class KafkaInternalActorSystems implements InternalActorSystems, ActorRefFactory {
    private final InternalActorSystems delegate;
    private final Cache<String,ActorRef> actorRefCache;
    private final KafkaActorRefTools actorRefTools;
    private final SystemSerializers systemSerializers = new SystemSerializers(this);
    private final SystemDeserializers systemDeserializers = new SystemDeserializers(this, this);

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
            actorRefCache.put(refSpec, actorRef);
        }
        return actorRef;
    }
}
