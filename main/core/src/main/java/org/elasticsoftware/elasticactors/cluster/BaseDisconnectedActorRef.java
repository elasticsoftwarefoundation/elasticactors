/*
 * Copyright 2013 - 2023 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.concurrent.ActorCompletableFuture;
import org.reactivestreams.Publisher;

import jakarta.annotation.Nullable;

/**
 * @author Joost van de Wijgerd
 */
public abstract class BaseDisconnectedActorRef implements ActorRef, ActorContainerRef {
    protected final String clusterName;
    protected final String actorId;
    protected final String refSpec;
    protected final String actorSystemName;

    public BaseDisconnectedActorRef(@Nullable String actorId, String clusterName, String refSpec, String actorSystemName) {
        this.actorId = actorId;
        this.clusterName = clusterName;
        this.refSpec = refSpec;
        this.actorSystemName = actorSystemName;
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
    public final void tell(Object message, ActorRef sender) {
        tell(message);
    }

    @Override
    public final void tell(Object message) {
        throw new IllegalStateException(getExceptionMessage());
    }

    protected abstract String getExceptionMessage();

    @Override
    public final <T> ActorCompletableFuture<T> ask(Object message, Class<T> responseType) {
        ActorCompletableFuture<T> future = new ActorCompletableFuture<>();
        future.completeExceptionally(new IllegalStateException(getExceptionMessage()));
        return future;
    }

    @Override
    public <T> ActorCompletableFuture<T> ask(Object message, Class<T> responseType, Boolean persistOnResponse) {
        ActorCompletableFuture<T> future = new ActorCompletableFuture<>();
        future.completeExceptionally(new IllegalStateException(getExceptionMessage()));
        return future;
    }

    @Override
    public <T> Publisher<T> publisherOf(Class<T> messageClass) {
        return s -> s.onError(new IllegalStateException(getExceptionMessage()));
    }

    @Override
    public final boolean equals(Object o) {
        return this == o || o instanceof ActorRef && this.toString().equals(o.toString());
    }

    @Override
    public final int hashCode() {
        return toString().hashCode();
    }

    @Override
    public final String toString() {
        return this.refSpec;
    }

    @Override
    public final boolean isLocal() {
        return false;
    }

    @Override
    public final ActorContainer getActorContainer() {
        throw new IllegalStateException(getExceptionMessage());
    }
}
