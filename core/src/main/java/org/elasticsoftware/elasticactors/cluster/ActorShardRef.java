/*
 * Copyright 2013 - 2017 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.SubscribeMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.SubscriptionMessage;
import org.elasticsoftware.elasticactors.reactive.PersistentSubscriptionImpl;
import org.elasticsoftware.elasticactors.serialization.NoopSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsoftware.elasticactors.ActorContextHolder.getSelf;

/**
 * {@link org.elasticsoftware.elasticactors.ActorRef} that references an actor in the local cluster
 *
 * @author  Joost van de Wijgerd
 */
public final class ActorShardRef implements ActorRef, ActorContainerRef {
    private final InternalActorSystem actorSystem;
    private final String clusterName;
    private final ActorShard shard;
    private final String actorId;
    private final String refSpec;

    public ActorShardRef(InternalActorSystem actorSystem, String clusterName, ActorShard shard, @Nullable String actorId) {
        this.actorSystem = actorSystem;
        this.clusterName = clusterName;
        this.shard = shard;
        this.actorId = actorId;
        this.refSpec = generateRefSpec(clusterName, shard, actorId);
    }

    public ActorShardRef(InternalActorSystem actorSystem, String clusterName, ActorShard shard) {
        this(actorSystem, clusterName, shard, null);
    }

    public static String generateRefSpec(String clusterName,ActorShard shard,@Nullable String actorId) {
        if(actorId != null) {
            return String.format("actor://%s/%s/shards/%d/%s",
                    clusterName,shard.getKey().getActorSystemName(),
                    shard.getKey().getShardId(),actorId);
        } else {
            return String.format("actor://%s/%s/shards/%d",
                    clusterName,shard.getKey().getActorSystemName(),
                    shard.getKey().getShardId());
        }
    }

    @Override
    public String getActorCluster() {
        return clusterName;
    }

    @Override
    public String getActorPath() {
        return String.format("%s/shards/%d",shard.getKey().getActorSystemName(),shard.getKey().getShardId());
    }

    public String getActorId() {
        return actorId;
    }

    @Override
    public void tell(Object message, ActorRef sender) {
        try {
            shard.sendMessage(sender,this,message);
        } catch(MessageDeliveryException e) {
            throw e;
        } catch (Exception e) {
            throw new MessageDeliveryException("Unexpected Exception while sending message",e,false);
        }
    }

    @Override
    public void tell(Object message) {
        final ActorRef self = getSelf();
        if(self != null) {
            tell(message,self);
        } else {
            throw new IllegalStateException("Cannot determine ActorRef(self) Only use this method while inside an ElasticActor Lifecycle or on(Message) method!");
        }
    }

    @Override
    public boolean isLocal() {
        return shard.getOwningNode().isLocal();
    }

    @Override
    public ActorContainer getActorContainer() {
        return shard;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof ActorRef && this.refSpec.equals(o.toString());
    }

    @Override
    public int hashCode() {
        return this.refSpec.hashCode();
    }

    @Override
    public String toString() {
        return this.refSpec;
    }

    @Override
    public <T> Publisher<T> publisherOf(String messageName) {
        return new Pub<>(messageName);
    }

    @TempActor(stateClass = Pub.class)
    private final class ReplyActor<T> extends TypedActor<T> {
        @Override
        public void onUndeliverable(ActorRef receiver, Object message) throws Exception {
            final Pub delegate = getState(Pub.class);
            delegate.onUndeliverable(receiver,message);
        }

        @Override
        public void onReceive(ActorRef sender, T message) throws Exception {
            final Pub delegate = getState(Pub.class);
            delegate.onReceive(sender, message);
        }
    }

    private final class Pub<T> extends TypedActor<Object> implements ActorState<Pub<T>>, Publisher<T> {
        private final String messageName;
        private final AtomicReference<Subscriber<? super T>> subscriber = new AtomicReference<>();
        private final ActorRef replyRef;

        Pub(String messageName) {
            this.messageName = messageName;
            try {
                this.replyRef = actorSystem.tempActorOf(ReplyActor.class, this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void subscribe(Subscriber<? super T> subscriber) {
            // store the subscriber (will be accessed by different thread)
            this.subscriber.set(subscriber);
            // start the subscription protocol
            tell(new SubscribeMessage(messageName), replyRef);
        }

        @Override
        public void onReceive(ActorRef sender, Object message) throws Exception {
            final Subscriber<? super T> subscriber = this.subscriber.get();
            if(message instanceof SubscriptionMessage) {
                // if the subscriber is an actor, we need to register the persistent subscription
                if(subscriber instanceof ElasticActor) {

                } else {
                    subscriber.onSubscribe(new PersistentSubscriptionImpl(ActorShardRef.this, messageName));
                }
            }
        }

        @Override
        public void onUndeliverable(ActorRef receiver, Object message) throws Exception {
            // the actor doesn't exist. this is an error
            // @todo this needs a proper exception
            final Subscriber<? super T> subscriber = this.subscriber.get();
            subscriber.onError(new RuntimeException("Publisher does not exist"));
            subscriber.onComplete();
            actorSystem.stop(replyRef);
        }

        @Override
        public Pub<T> getBody() {
            return this;
        }

        @Override
        public Class<? extends SerializationFramework> getSerializationFramework() {
            return NoopSerializationFramework.class;
        }
    }
}
