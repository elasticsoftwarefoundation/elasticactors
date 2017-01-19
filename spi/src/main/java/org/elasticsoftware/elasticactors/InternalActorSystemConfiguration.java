/*
 * Copyright 2013 - 2016 The Original Authors
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

import org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode;

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

    /**
     * Returns the default {@link org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode} that is used when
     * {@link org.elasticsoftware.elasticactors.serialization.Message#deliveryMode()} equals {@link org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode#SYSTEM_DEFAULT}
     *
     *
     * @return
     */
    MessageDeliveryMode getMessageDeliveryMode();
}
