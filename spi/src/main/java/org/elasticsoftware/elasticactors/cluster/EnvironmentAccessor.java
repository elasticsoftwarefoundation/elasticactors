package org.elasticsoftware.elasticactors.cluster;

import javax.annotation.Nullable;

/**
 * An environment accessor is able to get environment properties.
 * <br/>
 * This interface is modelled after Spring Framework's {@code PropertyResolver}.
 */
public interface EnvironmentAccessor {

    /**
     * Return the property value associated with the given key,
     * or {@code null} if the key cannot be resolved.
     * @param key the property name to resolve
     * @see #getProperty(String, String)
     * @see #getProperty(String, Class)
     * @see #getRequiredProperty(String)
     */
    @Nullable
    String getProperty(String key);

    /**
     * Return the property value associated with the given key, or
     * {@code defaultValue} if the key cannot be resolved.
     * @param key the property name to resolve
     * @param defaultValue the default value to return if no value is found
     * @see #getRequiredProperty(String)
     * @see #getProperty(String, Class)
     */
    String getProperty(String key, String defaultValue);

    /**
     * Return the property value associated with the given key,
     * or {@code null} if the key cannot be resolved.
     * @param key the property name to resolve
     * @param targetType the expected type of the property value
     * @see #getRequiredProperty(String, Class)
     */
    @Nullable
    <T> T getProperty(String key, Class<T> targetType);

    /**
     * Return the property value associated with the given key,
     * or {@code defaultValue} if the key cannot be resolved.
     * @param key the property name to resolve
     * @param targetType the expected type of the property value
     * @param defaultValue the default value to return if no value is found
     * @see #getRequiredProperty(String, Class)
     */
    <T> T getProperty(String key, Class<T> targetType, T defaultValue);

    /**
     * Return the property value associated with the given key (never {@code null}).
     * @throws IllegalStateException if the key cannot be resolved
     * @see #getRequiredProperty(String, Class)
     */
    String getRequiredProperty(String key) throws IllegalStateException;

    /**
     * Return the property value associated with the given key, converted to the given
     * targetType (never {@code null}).
     * @throws IllegalStateException if the given key cannot be resolved
     */
    <T> T getRequiredProperty(String key, Class<T> targetType) throws IllegalStateException;

}
