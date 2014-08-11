package org.elasticsoftware.elasticactors;

import javax.annotation.Nullable;

/**
 * @author Joost van de Wijgerd
 */
public interface ActorLifecycleListener<T extends ActorState> {
    Class<? extends ElasticActor> getActorClass();

    void postCreate(ActorRef actorRef,T actorState);

    void postActivate(ActorRef actorRef,T actorState, @Nullable String previousVersion);

    void prePassivate(ActorRef actorRef,T actorState);

    void preDestroy(ActorRef actorRef,T actorState);
}
