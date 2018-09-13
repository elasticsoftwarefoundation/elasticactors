package org.elasticsoftware.elasticactors.client;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.core.actors.CompletableFutureDelegate;
import org.elasticsoftware.elasticactors.core.actors.ReplyActor;
import org.reactivestreams.Publisher;

import java.util.concurrent.CompletableFuture;

public final class RemoteActorRef implements ActorRef {
    private final ActorSystemClient actorSystemClient;
    private final ActorShard actorShard;
    private final String actorId;

    RemoteActorRef(ActorSystemClient actorSystemClient, ActorShard actorShard, String actorId) {
        this.actorSystemClient = actorSystemClient;
        this.actorShard = actorShard;
        this.actorId = actorId;
    }

    @Override
    public String getActorCluster() {
        return actorSystemClient.getClusterName();
    }

    @Override
    public String getActorPath() {
        return String.format("%s/shards/%d", actorShard.getKey().getActorSystemName(), actorShard.getKey().getShardId());
    }

    @Override
    public String getActorId() {
        return actorId;
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        try {
            actorShard.sendMessage(sender,this,message);
        } catch(MessageDeliveryException e) {
            throw e;
        } catch (Exception e) {
            throw new MessageDeliveryException("Unexpected Exception while sending message", e, false);
        }
    }

    @Override
    public void tell(Object message) {
        final ActorRef self = ActorContextHolder.getSelf();
        if(self != null) {
            tell(message,self);
        } else {
            throw new IllegalStateException("Cannot determine ActorRef(self) Only use this method while inside an ElasticActor Lifecycle or on(Message) method!");
        }
    }

    public final <T> CompletableFuture<T> ask(Object message, Class<T> responseType) {
        return ask(message, responseType, Boolean.FALSE);
    }


    @Override
    public <T> CompletableFuture<T> ask(Object message, Class<T> responseType, Boolean persistOnResponse) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        try {
            ActorRef callerRef = Boolean.TRUE.equals(persistOnResponse) ? ActorContextHolder.getSelf() : null;
            ActorRef replyRef = actorSystemClient.tempActorOf(ReplyActor.class,
                    new CompletableFutureDelegate<>(future, responseType, callerRef));
            this.tell(message, replyRef);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public <T> Publisher<T> publisherOf(Class<T> messageClass) {
        throw new UnsupportedOperationException();
    }
}
