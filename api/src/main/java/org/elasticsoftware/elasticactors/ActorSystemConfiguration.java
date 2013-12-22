/*
 * Copyright 2013 the original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors;

import java.util.Set;

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
     * Return the singleton service instance
     *
     * @param serviceId
     * @return
     */
    ElasticActor<?> getService(String serviceId);

    /**
     * return a list of service actor id's
     *
     * @return
     */
    Set<String> getServices();

    /**
     * Return the String value of the given property, namespaced on the component class
     *
     * @param component
     * @param propertyName
     * @param defaultValue
     * @return
     */
    String getStringProperty(Class component,String propertyName,String defaultValue);

    Integer getIntegerProperty(Class component, String propertyName, int defaultValue);
}


