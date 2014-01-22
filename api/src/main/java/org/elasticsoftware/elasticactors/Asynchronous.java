package org.elasticsoftware.elasticactors;

import java.lang.annotation.*;

/**
 * Use this annotation to annotate methods of {@link ServiceActor} actors to make these methods run on a
 * different thread than the actor thread. Should be used when accessing resources such as URLs or Databases
 * that don't have an asynchronous API.
 *
 * @author Joost van de Wijgerd
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Asynchronous {
}
