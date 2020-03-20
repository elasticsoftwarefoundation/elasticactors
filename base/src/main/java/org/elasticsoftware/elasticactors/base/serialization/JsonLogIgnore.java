package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A custom annotation for preventing the serialization of annotated fields or methods when logging a message's
 * body. It is inspired by {@link JsonIgnore} and works in a similar fashion.
 */
@Documented
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface JsonLogIgnore {

}
