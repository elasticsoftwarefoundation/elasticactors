package org.elasticsoftware.elasticactors;

import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.state.NullActorState;

import java.lang.annotation.*;

/**
 * @author Joost van de Wijgerd
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Actor {
    Class<? extends ActorState> stateClass() default NullActorState.class;

    Class<? extends SerializationFramework> serializationFramework();
}
