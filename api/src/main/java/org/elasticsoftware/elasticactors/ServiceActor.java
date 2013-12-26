package org.elasticsoftware.elasticactors;

import javax.inject.Named;
import java.lang.annotation.*;

/**
 * Marks an actor as a Service Actor (i.e. one singleton instance per Actor Node)
 *
 * @author Joost van de Wijgerd
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Named
public @interface ServiceActor {
    String name() default "";
}
