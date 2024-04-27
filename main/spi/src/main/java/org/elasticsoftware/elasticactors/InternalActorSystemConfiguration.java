/*
 * Copyright 2013 - 2024 The Original Authors
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

import java.util.List;
import java.util.Set;

/**
 * @author Joost van de Wijgerd
 */
public interface InternalActorSystemConfiguration extends ActorSystemConfiguration {

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
     * Return the remote configurations
     *
     * @return
     */
    List<? extends RemoteActorSystemConfiguration> getRemoteConfigurations();
}
