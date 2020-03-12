package org.elasticsoftware.elasticactors.base.serialization;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A custom annotation for enable serialization of a field when logging an actor message's body.
 * This works in an inverse fashion relative to {@link JsonIgnore}. <br/> <br/>
 *
 * <strong>Only fields/methods annotated with this will be used for the serialization
 * when logging a message</strong>. This is to avoid logging sensitive data by requiring a
 * property/field to be explicitly annotated in order to be logged.
 *
 * If used on a class, all fields of that class will be serialized unless overridden.
 */
@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface JsonLog {

    /**
     * Optional argument that defines whether this annotation is active
     * or not. The only use for value 'false' if for overriding purposes
     * (which is not needed often); most likely it is needed for use
     * with "mix-in annotations" (aka "annotation overrides").
     * For most cases, however, default value of "true" is just fine
     * and should be omitted.
     *
     * @return True if annotation is enabled (normal case); false if it is to
     *   be ignored (only useful for mix-in annotations to "mask" annotation)
     */
    boolean value() default true;

}
