package org.elasticsoftware.elasticactors.test.common;

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.InitialStateProvider;

public class InitialNameStateProvider implements InitialStateProvider {

    @Override
    public ActorState<?> getInitialState(
            String actorId,
            Class<? extends ActorState> stateClass) throws Exception {
        return new NameActorState(actorId);
    }
}
