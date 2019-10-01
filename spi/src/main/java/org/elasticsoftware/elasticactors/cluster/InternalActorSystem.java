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

import org.elasticsoftware.elasticactors.ActorLifecycleListener;
import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.InternalActorSystemConfiguration;
import org.elasticsoftware.elasticactors.cluster.scheduler.InternalScheduler;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public interface InternalActorSystem extends ActorSystem, ShardAccessor {

    @Override
    InternalActorSystemConfiguration getConfiguration();

    /**
     * Return a reference to a temporary actor
     *
     * @param actorId
     * @return
     */
    ActorRef tempActorFor(String actorId);

    /**
     * Return the singleton instance of an {@link ElasticActor}
     *
     * @param actorRef
     * @param actorClass
     * @return
     */
    ElasticActor getActorInstance(ActorRef actorRef,Class<? extends ElasticActor> actorClass);

    /**
     * Return a service or null if the service was not found
     *
     * @param serviceRef
     * @return
     */
    ElasticActor getServiceInstance(ActorRef serviceRef);

    /**
     * Returns a {@link ActorNode} that can be either remote or local
     *
     * @param nodeId
     * @return
     */
    ActorNode getNode(String nodeId);

    /**
     * Returns the local {@link ActorNode}
     *
     * @return
     */
    ActorNode getNode();

    /**
     * Return the serializer for the given message type
     *
     * @param messageClass
     * @param <T>
     * @return
     */
    <T> MessageSerializer<T> getSerializer(Class<T> messageClass);

    /**
     * Return the deserializer for the give message type
     *
     * @param messageClass
     * @param <T>
     * @return
     */
    <T> MessageDeserializer<T> getDeserializer(Class<T> messageClass);

    InternalScheduler getInternalScheduler();

    @Override
    InternalActorSystems getParent();

    List<ActorLifecycleListener<?>> getActorLifecycleListeners(Class<? extends ElasticActor> actorClass);

    /**
     * Returns whether or not the actor system is currently stable, i.e. if all shards have been assigned and initialized
     * properly.
     *
     * @return true when the actor system is stable, false otherwise.
     */
    boolean isStable();
}
