package org.elasticsoftware.elasticactors.state;

import java.lang.annotation.*;

/**
 * @author Joost van de Wijgerd
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PersistenceConfig {
    boolean persistAll() default true;

    Class<?>[] excluded() default {};
}
