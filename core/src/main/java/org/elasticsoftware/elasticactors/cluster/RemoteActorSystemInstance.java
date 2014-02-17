package org.elasticsoftware.elasticactors.cluster;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author Joost van de Wijgerd
 */
public final class  RemoteActorSystemInstance implements ActorSystem, ShardAccessor {
    private final HashFunction hashFunction = Hashing.murmur3_32();
    private final RemoteActorSystemConfiguration configuration;
    private final InternalActorSystem localActorSystem;
    private final ActorShard[] shards;
    private final MessageQueueFactory messageQueueFactory;

    public RemoteActorSystemInstance(RemoteActorSystemConfiguration configuration,
                                     InternalActorSystem localActorSystem,
                                     MessageQueueFactory messageQueueFactory) {
        this.configuration = configuration;
        this.localActorSystem = localActorSystem;
        this.messageQueueFactory = messageQueueFactory;
        this.shards = new RemoteActorShard[configuration.getNumberOfShards()];
    }

    @PostConstruct
    public void init() throws Exception {
        for (int i = 0; i < shards.length; i++) {
            shards[i] = new RemoteActorSystemActorShard(localActorSystem,configuration.getClusterName(),configuration.getName(),i,messageQueueFactory);
        }
        for (ActorShard shard : shards) {
            shard.init();
        }
    }

    @PreDestroy
    public void destroy() {
        for (ActorShard shard : shards) {
            shard.destroy();
        }
    }

    @Override
    public String getName() {
        return configuration.getName();
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass) throws Exception {
        return actorOf(actorId, actorClass, null);
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass, ActorState initialState) throws Exception {
        // determine shard
        ActorShard shard = shardFor(actorId);
        // send CreateActorMessage to shard
        CreateActorMessage createActorMessage = new CreateActorMessage(getName(), actorClass.getName(), actorId, initialState);
        ActorRef creator = ActorContextHolder.getSelf();
        shard.sendMessage(creator, shard.getActorRef(), createActorMessage);
        // create actor ref
        // @todo: add cache to speed up performance
        return new ActorShardRef(configuration.getClusterName(), shard, actorId);
    }

    @Override
    public <T> ActorRef tempActorOf(Class<T> actorClass, ActorState initialState) throws Exception {
        throw new UnsupportedOperationException("Temporary Actors are not supported for Remote ActorSystem instances");
    }

    @Override
    public ActorRef actorFor(String actorId) {
        // determine shard
        ActorShard shard = shardFor(actorId);
        // return actor ref
        // @todo: add cache to speed up performance
        return new ActorShardRef(configuration.getClusterName(), shard, actorId);
    }

    @Override
    public ActorRef serviceActorFor(String actorId) {
        throw new UnsupportedOperationException("Service Actors are not supported for Remote ActorSystem instances");
    }

    @Override
    public Scheduler getScheduler() {
        throw new UnsupportedOperationException("Scheduler is not supported for Remote ActorSystem instances");
    }

    @Override
    public ActorSystems getParent() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void stop(ActorRef actorRef) throws Exception {
        // set sender if we have any in the current context
        ActorRef sender = ActorContextHolder.getSelf();
        ActorContainer handlingContainer = ((ActorContainerRef) actorRef).get();
        handlingContainer.sendMessage(sender, handlingContainer.getActorRef(), new DestroyActorMessage(actorRef));
    }

    @Override
    public ActorShard getShard(String actorPath) {
        // for now we support only <ActorSystemName>/shards/<shardId>
        // @todo: do this with actorRef tools
        String[] pathElements = actorPath.split("/");
        if (pathElements[1].equals("shards")) {
            return shards[Integer.parseInt(pathElements[2])];
        } else {
            throw new IllegalArgumentException(String.format("No ActorShard found for actorPath [%s]", actorPath));
        }
    }

    private ActorShard shardFor(String actorId) {
        return shards[Math.abs(hashFunction.hashString(actorId, Charsets.UTF_8).asInt()) % shards.length];
    }
}
