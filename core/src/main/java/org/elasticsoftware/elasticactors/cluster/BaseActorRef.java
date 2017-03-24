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

import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.core.actors.*;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.*;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.reactivestreams.Publisher;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * @author Joost van de Wijgerd
 */
public abstract class BaseActorRef implements ActorRef {
    protected final InternalActorSystem actorSystem;
    protected final String clusterName;
    protected final String actorId;
    protected final String refSpec;

    public BaseActorRef(InternalActorSystem actorSystem, String clusterName, @Nullable String actorId, String refSpec) {
        this.actorSystem = actorSystem;
        this.actorId = actorId;
        this.clusterName = clusterName;
        this.refSpec = refSpec;
    }

    public final <T> CompletableFuture<T> ask(Object message, Class<T> responseType) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        try {
            ActorRef replyRef = actorSystem.tempActorOf(ReplyActor.class, new CompletableFutureDelegate<>(future, responseType));
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
        if(ActorContextHolder.getSelf() == null) {
            return subscriber -> {
                try {
                    // ActorRef delegateRef = actorSystem.tempActorOf(ReplyActor.class, new SubscriberDelegate(BaseActorRef.this, messageName, subscriber));
                    ActorRef delegateRef = actorSystem.tempActorOf(SubscriberActor.class, new SubscriberState<>(subscriber));
                    tell(new SubscribeMessage(delegateRef, messageName), delegateRef);
                } catch (Exception e) {
                    subscriber.onError(e);
                }
            };
        } else {
            // for now it is not possible to use lambda's or other anonymous classes while inside and ActorContext
            // due to thread safety issue
            return subscriber -> {
                if(subscriber instanceof TypedActor.SubscriberRef) {
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
