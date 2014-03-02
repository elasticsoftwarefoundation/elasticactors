/*
 * Copyright 2013 - 2014 The Original Authors
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

package org.elasticsoftware.elasticactors;

/**
 * This is the main entry point for the ElasticActors API. When a {@link org.elasticsoftware.elasticactors.serialization.Message}
 * needs to be send to an {@link ElasticActor} first the {@link ActorRef} needs to be obtained. An Actor Reference
 * can be obtained by using the sender field from {@link ElasticActor#onReceive(ActorRef, Object)} or by getting an
 * {@link ActorRef} from the various methods on {@link ActorSystem}.
 *
 * An {@link ActorRef} in serialized form looks as follows:<br/>
 *
 * (Normal) Actors: actor://<cluster>/<actorSystem>/shards/<shardId>/<actorId><br/>
 * Service Actors: actor://<cluster>/<actorSystem>/nodes/<nodeId>/<serviceId><br/>
 * Temporary Actors: actor://<cluster>/<actorSystem>/nodes/<nodeId>/<actorId><br/>
 *
 *
 * @see     {@link ActorSystem#actorFor(String)}
 * @see     {@link ActorSystem#actorOf(String, Class, ActorState)}
 * @see     {@link ActorSystem#tempActorOf(Class, ActorState)}
 * @see     {@link ActorSystem#serviceActorFor(String)}
 *
 * @author Joost van de Wijgerd
 */
public interface ActorRef {
    /**
     * The path of an actor is the Shard (or Node) where the actor is located. It looks like:
     * <actorSystem>/shards/<shardId> or <actorSystem>/nodes/<nodeId>
     *
     * @return  the actor path
     */
    String getActorPath();

    /**
     * The actor id is what the Developer names the Actor, or in case of Temporary Actors it's a
     * {@link java.util.UUID} that's assigned by the Runtime
     *
     * @see         {@link ActorSystem#actorFor(String)}
     * @see         {@link ActorSystem#tempActorOf(Class, ActorState)}
     * @see         {@link ActorSystem#serviceActorFor(String)}
     * @return      the actor id for this {@link ActorRef}
     */
    String getActorId();

    /**
     * This is the main method to send a message to an {@link ElasticActor}.
     *
     * @param message       the message to send (needs to be annotated with {@link org.elasticsoftware.elasticactors.serialization.Message}
     * @param sender        the sender, this can be self, but it can also be another {@link ActorRef}
     */
    void tell(Object message, ActorRef sender);

    /**
     * Equivalent to calling ActorRef.tell(message,getSelf())
     *
     * @param message       the message to send (needs to be annotated with {@link org.elasticsoftware.elasticactors.serialization.Message}
     * @throws  IllegalStateException if the method is not called withing a {@link ElasticActor} lifecycle or on(Message) method
     */
    void tell(Object message) throws IllegalStateException;
}
