package org.elasticsoftware.elasticactors.serialization;

import java.lang.annotation.*;

/**
 * @author Joost van de Wijgerd
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Message {
    Class<? extends SerializationFramework> serializationFramework();

    boolean durable() default true;
}
