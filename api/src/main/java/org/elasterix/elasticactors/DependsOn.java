package org.elasterix.elasticactors;

import java.lang.annotation.*;

/**
 * @author Joost van de Wijgerd
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DependsOn {
    String[] dependencies() default {};
}
