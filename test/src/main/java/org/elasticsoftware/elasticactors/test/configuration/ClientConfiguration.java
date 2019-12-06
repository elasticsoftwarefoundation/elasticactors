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

package org.elasticsoftware.elasticactors.test.configuration;

import org.elasticsoftware.elasticactors.RemoteActorSystemConfiguration;
import org.elasticsoftware.elasticactors.client.cluster.RemoteActorSystemInstance;
import org.elasticsoftware.elasticactors.client.serialization.ClientSerializationFrameworks;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.springframework.context.annotation.Bean;

import java.util.List;

public class ClientConfiguration {

    @Bean
    public RemoteActorSystemInstance clientActorSystem(
            RemoteActorSystemConfiguration configuration,
            ActorRefFactory actorRefFactory,
            MessageQueueFactory localMessageQueueFactory,
            List<SerializationFramework> serializationFrameworks) {
        return new RemoteActorSystemInstance(
                configuration,
                new ClientSerializationFrameworks(actorRefFactory, serializationFrameworks),
                localMessageQueueFactory);
    }

}
