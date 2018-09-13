package org.elasticsoftware.elasticactors;

import javax.annotation.Nullable;

public interface ActorSystemClient {
    String getClusterName();

    /**
     * The name of this {@link ActorSystem}. The name is unique within the same cluster
     *
     * @return
     */
    String getName();

    /**
     * Create a Temporary Actor with the given initial {@link ActorState}. A Temp Actor is an {@link ElasticActor}
     * instance that is annotated with {@link TempActor}. Temporary Actors are located on the Local ElasticActors
     * node only and will not survive a restart of the Node. Temporary Actors are most commonly used to bridge between
     * two systems, for instance within a Controller. It will implement a Reply Address.<br/>
     * A Temporary Actor will get and actorId assigned by the Runtime. The resulting {@link ActorRef} can be stored however
     * due to the ephemeral nature of the Temporary Actor this is not advisable.
     *
     * @param actorClass    the type class of the actor. Needs to be annotated with {@link TempActor}
     * @param initialState  the initial state, should be of type {@link org.elasticsoftware.elasticactors.TempActor#stateClass()}
     * @param <T>           generic type info
     * @throws Exception    when something unexpected happens
     * @return              the {@link ActorRef} pointing to the newly created actor
     */
    <T> ActorRef tempActorOf(Class<T> actorClass,@Nullable ActorState initialState) throws Exception;

    /**
     * Return an {@link ActorRef} to a (Standard) Actor. There is no guarantee that the Actor actually exists. If you need
     * to be sure that the Actor exists call: {@link ActorSystem#actorOf(String, Class, ActorState)}. This is an idempotent
     * operation that won't overwrite the {@link ActorState} if it already exists so it's safe to use in all cases. However
     * there is significant overhead to this so if you are sure the the Actor exists then it's better to use this method.<br/>
     * This can be used only to get an {@link ActorRef} to a (Standard) {@link ElasticActor}.
     * For Service Actors use {@link ActorSystem#serviceActorFor(String)}
     *
     * @param actorId   the id of the (Standard) Actor to reference
     * @return          the {@link ActorRef} to the Actor (not guaranteed to exist!)
     */
    ActorRef actorFor(String actorId);

}
