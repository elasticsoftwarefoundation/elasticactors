package org.elasticsoftware.elasticactors;

import org.elasticsoftware.elasticactors.state.NullActorState;

import java.lang.annotation.*;

/**
 * Marks an actor as a Temporary Actor (i.e. it's state is not persistent and it won't survive restarts)
 *
 * @author Joost van de Wijgerd
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TempActor {
    Class<? extends ActorState> stateClass() default NullActorState.class;
}
