package org.elasticsoftware.elasticactors;

/**
 * @author Joost van de Wijgerd
 */
public interface ActorLifecycleListener<T extends ActorState> {
    Class<? extends ElasticActor> getActorClass();

    void postCreate(ActorRef actorRef,T actorState);

    void postActivate(ActorRef actorRef,T actorState);

    void prePassivate(ActorRef actorRef,T actorState);

    void preDestroy(ActorRef actorRef,T actorState);
}
