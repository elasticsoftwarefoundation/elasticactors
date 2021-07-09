package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A custom annotation for preventing the serialization of selected fields when logging a message's
 * body. It is inspired by {@link JsonIgnoreProperties} and works in a similar fashion.
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface JsonLogIgnoreProperties {

    /**
     * Names of properties to ignore.
     */
    String[] value() default {};

}
