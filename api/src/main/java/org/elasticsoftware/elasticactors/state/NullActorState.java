package org.elasticsoftware.elasticactors.state;

import org.elasticsoftware.elasticactors.ActorState;

/**
 * @author Joost van de Wijgerd
 */
public final class NullActorState implements ActorState<Void,NullActorState> {
    @Override
    public Void getId() {
        return null;
    }

    @Override
    public NullActorState getBody() {
        return this;
    }
}
