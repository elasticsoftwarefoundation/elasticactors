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

package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.TypedSubscriber;
import org.elasticsoftware.elasticactors.core.actors.CompletableFutureDelegate;
import org.elasticsoftware.elasticactors.core.actors.ReplyActor;
import org.elasticsoftware.elasticactors.core.actors.SubscriberActor;
import org.elasticsoftware.elasticactors.core.actors.SubscriberState;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.SubscribeMessage;
import org.elasticsoftware.elasticactors.reactivestreams.PersistentSubscriptionImpl;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.reactivestreams.Publisher;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

import static org.elasticsoftware.elasticactors.cluster.tasks.InternalActorContext.getAsProcessorContext;

/**
 * @author Joost van de Wijgerd
 */
public abstract class BaseActorRef implements ActorRef {
    protected final InternalActorSystem actorSystem;
    protected final String clusterName;
    protected final String actorId;
    protected final String refSpec;

    protected BaseActorRef(InternalActorSystem actorSystem, String clusterName, @Nullable String actorId, String refSpec) {
        this.actorSystem = actorSystem;
        this.actorId = actorId;
        this.clusterName = clusterName;
        this.refSpec = refSpec;
    }

    @Override
    public final <T> CompletableFuture<T> ask(Object message, Class<T> responseType) {
        return ask(message, responseType, Boolean.FALSE);
    }

    @Override
    public <T> CompletableFuture<T> ask(Object message, Class<T> responseType, Boolean persistOnResponse) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        try {
            ActorRef callerRef = Boolean.TRUE.equals(persistOnResponse) ? ActorContextHolder.getSelf() : null;
            ActorRef replyRef = actorSystem.tempActorOf(ReplyActor.class,
                                                        new CompletableFutureDelegate<>(future, responseType, callerRef));
            this.tell(message, replyRef);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public <T> Publisher<T> publisherOf(Class<T> messageClass) {
        if(messageClass.getAnnotation(Message.class) == null) {
            throw new IllegalArgumentException("messageClass needs to be annotated with @Message");
        }
        final String messageName = messageClass.getName();
        // see if we are in an Actor Context or not
        if(!ActorContextHolder.hasActorContext()) {
            return subscriber -> {
                try {
                    actorSystem.tempActorOf(SubscriberActor.class, new SubscriberState<>(subscriber, this, messageName));
                } catch (Exception e) {
                    subscriber.onError(e);
                }
            };
        } else {
            // for now it is not possible to use lambda's or other anonymous classes while inside and ActorContext
            // due to thread safety issue
            return subscriber -> {
                if(subscriber instanceof TypedSubscriber) {
                    // prepare the subscription (will be not active at this point)
                    getAsProcessorContext().addSubscription(new PersistentSubscriptionImpl(ActorContextHolder.getSelf(), this, messageName, subscriber));
                    // all is good, start the protocol handshake
                    tell(new SubscribeMessage(ActorContextHolder.getSelf(), messageName), ActorContextHolder.getSelf());
                } else {
                    subscriber.onError(new IllegalStateException("Within the context of an Actor it is not possible to use lambda's or anonymous classes. Please use this.asSubscriber() to pass in the proper reference"));
                }
            };
        }
    }

    @Override
    public final String getActorCluster() {
        return clusterName;
    }

    @Override
    public final String getActorId() {
        return actorId;
    }

    @Override
    public final boolean equals(Object o) {
        return this == o || o instanceof ActorRef && this.refSpec.equals(o.toString());
    }

    @Override
    public final int hashCode() {
        return this.refSpec.hashCode();
    }

    @Override
    public final String toString() {
        return this.refSpec;
    }



}
