package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A custom annotation for enable serialization of selected fields when logging an actor message's
 * body. This works in an inverse fashion relative to {@link JsonIgnoreProperties}.
 */
@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface JsonLogProperties {

    /**
     * Names of properties to include.
     */
    String[] value() default {};

}
