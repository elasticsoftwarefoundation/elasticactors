/*
 * Copyright 2013 - 2023 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * @author Joost van de Wijgerd
 */
public interface ActorSystemConfiguration {
    /**
     * The name of this {@link ActorSystem}. The name has to be unique within the same cluster
     *
     * @return the name of this {@link ActorSystem}
     */
    @Nonnull
    String getName();

    /**
     * The number of shards. This determines how big an {@link ActorSystem} can scale. If a cluster
     * contains more nodes than shards then not every node will have a shard.
     *
     * THIS NUMBER MUST STAY THE SAME THROUGHOUT THE LIFE OF THIS ACTOR SYSTEM.
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
     * Never decrease this value, unless your system can deal with messages being lost.
     * Changing this number might cause messages to a shard actor to be sent to different queues
     * when those cross the node boundary. They are still going to be sent to the right shard, but
     * ordering cannot be guaranteed anymore, even when only using durable messages.
     * It's highly advised to scale your cluster down to 1 and do a rolling release or
     * completely restart it.
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
     * Never decrease this value, unless your system can deal with messages being lost.
     * Changing this number might cause messages to a node actor to be sent to different queues
     * when those cross the node boundary. It's highly advised to scale your cluster down to 1
     * and do a rolling release or completely restart it.
     *
     * This defaults to 1.
     */
    int getQueuesPerNode();

    /**
     * The seed to use for the hashing algorithm used to determine in which shard an actor resides.
     *
     * The best values to use are generally prime numbers.
     * This, however, defaults to 0 for historical reasons.
     *
     * THIS NUMBER MUST STAY THE SAME THROUGHOUT THE LIFE OF THIS ACTOR SYSTEM.
     * Changing it can be and likely will be catastrophic.
     */
    int getShardHashSeed();

    /**
     * The seed to use for the hashing algorithm used to determine in which queue to publish
     * messages when using more than one queue per node or shard.
     *
     * The best values to use are generally prime numbers.
     * This defaults to 53.
     *
     * THIS NUMBER MUST STAY THE SAME THROUGHOUT THE LIFE OF THIS ACTOR SYSTEM.
     * Changing it can be and likely will be catastrophic.
     */
    int getMultiQueueHashSeed();

    /**
     * The seed to use for the hashing algorithm used to determine which node will handle which shards.
     *
     * The best values to use are generally prime numbers.
     * Thie defaults to 53.
     *
     * This number can be changed, but your system must either be scaled down to 1 node before
     * being restarted; or it should be destroyed completely and rebuilt.
     *
     * Changing this number and doing a rolling release can result in two nodes handling the same
     * shard at the same time, which will be catastrophic.
     *
     * For historical reasons, this defaults to 0.
     */
    int getShardDistributionHashSeed();

    /**
     * The version of the ActorSystem
     *
     * @return the version of the ActorSystem
     */
    @Nonnull
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
    @Nullable
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
    @Nonnull
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
       @SuppressWarnings("unused")
       @Nonnull
       <T> T getRequiredProperty(Class component,String key, Class<T> targetType) throws IllegalStateException;

}


