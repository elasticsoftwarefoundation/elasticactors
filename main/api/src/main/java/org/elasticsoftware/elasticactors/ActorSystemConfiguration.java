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
     * @return the name of this {@link ActorSystem}
     */
    String getName();

    /**
     * The number of shards. This determines how big an {@link ActorSystem} can scale. If a cluster
     * contains more nodes than shards then not every node will have a shard.
     *
     * @return the number of shards
     */
    int getNumberOfShards();

    /**
     * The number of queues per shard.
     * If an {@link ActorSystem} is required to scale beyond its original number of shards without
     * resharding the persistence layer, this number can be increased.
     * The effect is that more message queues will be used for each shard.
     *
     * Beware that this can introduce limitations on the message broke because this
     * multiplies the number of queues used in it. For example, setting this to 2 will cause a
     * 256-shard {@link ActorSystem} to produce 512 shard queues.
     *
     * This defaults to 1.
     */
    int getQueuesPerShard();

    /**
     * The number of queues per node.
     *
     * If an {@link ActorSystem} node or node-bound actors (such as {@link TempActor} or
     * {@link ServiceActor}) are currently bottlenecking the performance of the {@link ActorSystem},
     * increasing this number will cause more message queues to be used for each node.
     *
     * Beware that this can introduce limitations on the message broker because this
     * multiplies the number of queues used in it. For example, setting this to 2 will cause a
     * 5-node {@link ActorSystem} to produce 10 node queues.
     *
     * This defaults to 1.
     */
    int getQueuesPerNode();

    /**
     * The version of the ActorSystem
     *
     * @return the version of the ActorSystem
     */
    String getVersion();

    /**
     * Return the property value associated with the given key, or {@code null}
     * if the key cannot be resolved.
     *
     * @param component the component class for which the key is resolved
     * @param key the property name to resolve
     * @param targetType the expected type of the property value
     * @param <T> the expected type of the property value
     * @return the property value associated with the given key, or {@code null} if the key cannot be resolved.
     * @see #getRequiredProperty(Class, String, Class)
     */
    <T> T getProperty(Class component,String key, Class<T> targetType);

    /**
     * Return the property value associated with the given key, or
     * {@code defaultValue} if the key cannot be resolved.
     *
     * @param component the component class for which the key is resolved
     * @param key the property name to resolve
     * @param targetType the expected type of the property value
     * @param defaultValue the default value to return if no value is found
     * @param <T> the expected type of the property value
     * @return the property value associated with the given key, or {@code defaultValue} if the key cannot be resolved.
     * @see #getRequiredProperty(Class,String, Class)
     */
    <T> T getProperty(Class component,String key, Class<T> targetType, T defaultValue);

   	/**
   	 * Return the property value associated with the given key, converted to the given
   	 * {@code targetType} (never {@code null}).
     *
     * @param component the component class for which the key is resolved
     * @param key the property name to resolve
     * @param targetType the expected type of the property value
     * @param <T> the expected type of the property value
     * @return the property value associated with the given key, converted to the given {@code targetType} (never {@code null}).
   	 * @throws IllegalStateException if the given key cannot be resolved
   	 */
   	<T> T getRequiredProperty(Class component,String key, Class<T> targetType) throws IllegalStateException;

}


