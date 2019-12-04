package org.elasticsoftware.elasticactors.client;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorRefGroup;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ActorSystemConfiguration;
import org.elasticsoftware.elasticactors.ActorSystems;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerRegistry;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

public class ClientActorSystem implements ActorSystem {

    private final HashFunction hashFunction = Hashing.murmur3_32();
    private final String clusterName;
    private final ActorSystemConfiguration configuration;
    private final ActorShard[] shards;

    public ClientActorSystem(
            String clusterName,
            ActorSystemConfiguration configuration,
            SerializationFrameworkCache serializationFrameworkCache,
            MessageQueueFactory messageQueueFactory) {
        this.clusterName = clusterName;
        this.configuration = configuration;
        this.shards = new ActorShard[configuration.getNumberOfShards()];
        for (int i = 0; i < configuration.getNumberOfShards(); i++) {
            this.shards[i] = new ClientActorShard(
                    new ShardKey(configuration.getName(), i),
                    messageQueueFactory,
                    serializationFrameworkCache);
        }
    }

    @Override
    public String getName() {
        return configuration.getName();
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass, ActorState initialState)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName, ActorState initialState)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public ActorRef actorOf(
            String actorId,
            String actorClassName,
            ActorState initialState,
            ActorRef creatorRef) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> ActorRef tempActorOf(Class<T> actorClass, @Nullable ActorState initialState)
            throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public ActorRef actorFor(String actorId) {
        int shardNum = Math.abs(hashFunction.hashString(actorId, StandardCharsets.UTF_8).asInt())
                % configuration.getNumberOfShards();
        return new ClientActorRef(clusterName, shards[shardNum], actorId);
    }

    @Override
    public ActorRef serviceActorFor(String actorId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ActorRef serviceActorFor(String nodeId, String actorId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Scheduler getScheduler() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ActorSystems getParent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop(ActorRef actorRef) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public ActorSystemConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public ActorSystemEventListenerRegistry getEventListenerRegistry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ActorRefGroup groupOf(Collection<ActorRef> members) throws IllegalArgumentException {
        throw new UnsupportedOperationException();
    }

    @PreDestroy
    public void destroy() {
        for (ActorShard shard : shards) {
            shard.destroy();
        }
    }

}
