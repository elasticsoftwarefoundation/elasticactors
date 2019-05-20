/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors;

/**
 * @author Joost van de Wijgerd
 */
public interface ActorSystemConfiguration {
    /**
     * The name of this {@link ActorSystem}. The name has to be unique within the same cluster
     *
     * @return
     */
    String getName();

    /**
     * The number of shards. This determines how big an {@link org.elasticsoftware.elasticactors.ActorSystem} can scale. If a cluster
     * contains more nodes than shards then not every node will have a shard.
     *
     * @return
     */
    int getNumberOfShards();

    /**
     * The version of the ActorSystem
     *
     * @return
     */
    String getVersion();

    /**
     * Return the property value associated with the given key, or {@code null}
     * if the key cannot be resolved.
     * @param key the property name to resolve
     * @param targetType the expected type of the property value
     * @see #getRequiredProperty(Class, String, Class)
     */
    <T> T getProperty(Class component,String key, Class<T> targetType);

    /**
     * Return the property value associated with the given key, or
     * {@code defaultValue} if the key cannot be resolved.
     * @param key the property name to resolve
     * @param targetType the expected type of the property value
     * @param defaultValue the default value to return if no value is found
     * @see #getRequiredProperty(Class,String, Class)
     */
    <T> T getProperty(Class component,String key, Class<T> targetType, T defaultValue);

   	/**
   	 * Return the property value associated with the given key, converted to the given
   	 * targetType (never {@code null}).
   	 * @throws IllegalStateException if the given key cannot be resolved
   	 */
   	<T> T getRequiredProperty(Class component,String key, Class<T> targetType) throws IllegalStateException;

}


