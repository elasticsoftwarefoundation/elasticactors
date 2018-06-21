package org.elasticsoftware.elasticactors;

public interface ActorSystemClient {
    String getClusterName();

    /**
     * The name of this {@link ActorSystem}. The name is unique within the same cluster
     *
     * @return
     */
    String getName();

    /**
     * Create a new {@link ElasticActor} of the given type. There will be no state passed (null). This is
     * an asynchronous method that could potentially fail. However the {@link ActorRef} is fully functional
     * and can be used to send messages immediately. All messages are handled on the same thread and will be
     * executed in order from the perspective of the receiving {@link ElasticActor}<br/>
     * This is a idempotent method, when an actor with the same actorId already exists, the method will silently
     * succeed. However the {@link ActorState} will NOT be overwritten.
     *
     * This method takes the name of the actor class as a parameter. This is mainly handy in the case where actors need
     * to be created on a remote ActorSystem. There is no strong type checking on the actor class and also no exception
     * will be thrown if the class doesn't exist on the remote end.
     *
     * When you are dealing with local actors, always prefer the stronger typed version of this method
     *
     * @param actorId           the actorId of the actor to create
     * @param actorClassName    the type class name of the actor. Needs to be annotated with {@link Actor}
     * @return                  the {@link ActorRef} pointing to the newly created actor
     * @throws Exception        when something unexpected happens
     */
    ActorRef actorOf(String actorId, String actorClassName) throws Exception;

    /**
     * Create a new {@link ElasticActor} of the given type with the given initial {@link ActorState}. This is
     * an asynchronous method that could potentially fail. However the {@link ActorRef} is fully functional
     * and can be used to send messages immediately. All messages are handled on the same thread and will be
     * executed in order from the perspective of the receiving {@link ElasticActor}<br/>
     * This is a idempotent method, when an actor with the same actorId already exists, the method will silently
     * succeed. However the {@link ActorState} will NOT be overwritten.
     *
     * This method takes the name of the actor class as a parameter. This is mainly handy in the case where actors need
     * to be created on a remote ActorSystem. There is no strong type checking on the actor class and also no exception
     * will be thrown if the class doesn't exist on the remote end.
     *
     * When you are dealing with local actors, always prefer the stronger typed version of this method
     *
     * @param actorId           the actorId of the actor to create
     * @param actorClassName    the type class name of the actor. Needs to be annotated with {@link Actor}
     * @param initialState      the initial state, should be of type {@link org.elasticsoftware.elasticactors.Actor#stateClass()}
     * @return                  the {@link ActorRef} pointing to the newly created actor
     * @throws Exception        when something unexpected happens
     */
    ActorRef actorOf(String actorId, String actorClassName, ActorState initialState) throws Exception;

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

    /**
     * Stop (destroy) the Actor that behind the {@link ActorRef}. Cannot be used with Service Actors. This is a
     * irreversible operation. The state will be destroyed and the {@link org.elasticsoftware.elasticactors.ActorRef#getActorId()}
     * will be deleted from the registry.
     *
     * @param actorRef          the {@link ActorRef} to destroy
     * @throws Exception        when something unexpected happens
     */
    void stop(ActorRef actorRef) throws Exception;
}
