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

package org.elasticsoftware.elasticactors.client;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.reactivestreams.Publisher;

import java.util.concurrent.CompletionStage;

public final class ClientActorRef implements ActorRef {

    private final String clusterName;
    private final ActorShard shard;
    private final String actorId;
    private final String refSpec;

    ClientActorRef(
            String clusterName,
            ActorShard shard,
            String actorId) {
        this.clusterName = clusterName;
        this.shard = shard;
        this.actorId = actorId;
        this.refSpec = String.format(
                "actor://%s/%s/shards/%d",
                clusterName,
                shard.getKey().getActorSystemName(),
                shard.getKey().getShardId());
    }

    @Override
    public String getActorCluster() {
        return clusterName;
    }

    @Override
    public String getActorPath() {
        return String.format(
                "%s/nodes/%s",
                shard.getKey().getActorSystemName(),
                shard.getKey().getShardId());
    }

    @Override
    public String getActorId() {
        return actorId;
    }

    @Override
    public void tell(Object message, ActorRef sender) throws MessageDeliveryException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void tell(Object message) throws IllegalStateException, MessageDeliveryException {
        try {
            shard.sendMessage(null, this, message);
        } catch (MessageDeliveryException e) {
            throw e;
        } catch (Exception e) {
            throw new MessageDeliveryException(
                    "Unexpected Exception while sending message",
                    e,
                    false);
        }
    }

    @Override
    public <T> CompletionStage<T> ask(
            Object message, Class<T> responseType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> CompletionStage<T> ask(
            Object message, Class<T> responseType, Boolean persistOnResponse) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public <T> Publisher<T> publisherOf(Class<T> messageClass) {
        throw new UnsupportedOperationException();
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
