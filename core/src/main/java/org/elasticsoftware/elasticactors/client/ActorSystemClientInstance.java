package org.elasticsoftware.elasticactors.client;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.RemoteActorShard;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.internal.ActorType;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.PostConstruct;
import java.util.UUID;

@Configurable
public class ActorSystemClientInstance implements ActorSystemClient {
    private final RemoteActorShard[] actorShards;
    private MessageQueueFactory localMessageQueueFactory;
    private MessageQueueFactory remoteMessageQueueFactory;
    private final String clusterName;
    private final String name;
    private final int numberOfShards;
    private final HashFunction hashFunction = Hashing.murmur3_32();
    private final ClientActorNode actorNode;

    public ActorSystemClientInstance(String clusterName, String name, int numberOfShards, ClientActorNode actorNode) {
        this.clusterName = clusterName;
        this.name = name;
        this.numberOfShards = numberOfShards;
        actorShards = new RemoteActorShard[this.numberOfShards];
        this.actorNode = actorNode;
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public <T> ActorRef tempActorOf(Class<T> actorClass, ActorState initialState) throws Exception {
        if(actorClass.getAnnotation(TempActor.class) == null) {
            throw new IllegalArgumentException("actorClass has to be annotated with @TempActor");
        }
        // if we have state we need to wrap it
        String actorId = UUID.randomUUID().toString();
        // see if we are being called in the context of another actor (and set the affinity key)
        String affinityKey = ActorContextHolder.hasActorContext() ? ActorContextHolder.getSelf().getActorId() : null;
        CreateActorMessage createActorMessage = new CreateActorMessage(getName(),
                actorClass.getName(),
                actorId,
                initialState,
                ActorType.TEMP,
                affinityKey);
        this.actorNode.sendMessage(null, actorNode.getActorRef(), createActorMessage);
        return new LocalActorRef(this, actorNode, actorId);
    }

    @Override
    public ActorRef actorFor(String actorId) {
        return new RemoteActorRef(this, shardFor(actorId), actorId);
    }

    @PostConstruct
    public void initialize() {
        // @todo: create the remote shards and the local client node to handle replies

    }

    @Autowired
    public void setLocalMessageQueueFactory(@Qualifier("localMessageQueueFactory") MessageQueueFactory localMessageQueueFactory) {
        this.localMessageQueueFactory = localMessageQueueFactory;
    }

    @Autowired
    public void setRemoteMessageQueueFactory(@Qualifier("remoteMessageQueueFactory") MessageQueueFactory remoteMessageQueueFactory) {
        this.remoteMessageQueueFactory = remoteMessageQueueFactory;
    }

    private ActorShard shardFor(String actorId) {
        return actorShards[Math.abs(hashFunction.hashString(actorId, Charsets.UTF_8).asInt()) % actorShards.length];
    }

}
