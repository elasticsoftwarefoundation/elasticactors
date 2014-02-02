package org.elasticsoftware.elasticactors.state;

import java.lang.annotation.*;

import static org.elasticsoftware.elasticactors.state.ActorLifecycleStep.*;

/**
 * By adding this annotation to an {@link org.elasticsoftware.elasticactors.ElasticActor} class hints can be
 * provided to the persistence runtime when not to persist the state. By default, state will always be persisted
 * after every message. By adding this annotation this behavior can be customized on a per-actor type basis.
 *
 * @author Joost van de Wijgerd
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PersistenceConfig {
    /**
     * Determine what persistence strategy is after a message is handled. This can be fine-tuned by using
     * {@link #excluded()} array when {@link #persistOnMessages()} is true (the default) and {@link #included()}
     * when this field is false
     *
     * @return
     */
    boolean persistOnMessages() default true;

    /**
     * Provide a list of message classes (i.e. objects annotated with {@link org.elasticsoftware.elasticactors.serialization.Message})
     * that will not lead to a state change in the actor so the state will not be saved after this message type is handled.
     *
     * Only used when {@link #persistOnMessages()} is true;
     *
     * @return
     */
    Class<?>[] excluded() default {};

    /**
     * Provide a list of message classes (i.e. objects annotated with {@link org.elasticsoftware.elasticactors.serialization.Message})
     * that will lead to a state change in the actor so the state will be saved after this message type is handled.
     *
     * Only used when {@link #persistOnMessages()} is false;
     *
     * @return
     */
    Class<?>[] included() default {};

    /**
     * Used to control the persistence behavior after a {@link ActorLifecycleStep} has been fired. By default the state
     * will be saved after {@link ActorLifecycleStep#CREATE} and {@link ActorLifecycleStep#ACTIVATE}
     *
     * @see org.elasticsoftware.elasticactors.ElasticActor#postCreate(org.elasticsoftware.elasticactors.ActorRef)
     * @see org.elasticsoftware.elasticactors.ElasticActor#postActivate(String)
     * @see org.elasticsoftware.elasticactors.ElasticActor#prePassivate()
     * @see org.elasticsoftware.elasticactors.ElasticActor#preDestroy(org.elasticsoftware.elasticactors.ActorRef)  
     * @return
     */
    ActorLifecycleStep[] persistOn() default {CREATE,ACTIVATE};
}
