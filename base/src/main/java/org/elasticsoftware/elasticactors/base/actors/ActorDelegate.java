package org.elasticsoftware.elasticactors.base.actors;

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.TypedActor;

/**
 * @author Joost van de Wijgerd
 */
public abstract class ActorDelegate<T> extends TypedActor<T> implements ActorState<Void,ActorDelegate<T>> {
    @Override
    public Void getId() {
        return null;
    }

}
