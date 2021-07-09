package org.elasticsoftware.elasticactors;

/**
 * Used to provide an actor's initial state for instances managed by the framework.
 * Implementations must have a no-args constructor.
 */
public interface InitialStateProvider {

    class Default implements InitialStateProvider {

        @Override
        public ActorState getInitialState(
                String actorId,
                Class<? extends ActorState> stateClass) throws Exception {
            return stateClass.newInstance();
        }
    }

    ActorState<?> getInitialState(String actorId, Class<? extends ActorState> stateClass) throws Exception;

}
