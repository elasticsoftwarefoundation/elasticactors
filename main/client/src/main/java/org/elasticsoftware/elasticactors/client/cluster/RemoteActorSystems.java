/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.client.cluster;

import org.elasticsoftware.elasticactors.RemoteActorSystemConfiguration;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactoryFactory;
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class RemoteActorSystems {

    private static final Logger logger = LoggerFactory.getLogger(RemoteActorSystems.class);
    private final Map<String, RemoteActorSystemInstance> remoteActorSystems;

    public RemoteActorSystems(
            List<RemoteActorSystemConfiguration> remoteConfigurations,
            SerializationFrameworks serializationFrameworks,
            MessageQueueFactoryFactory messageQueueFactoryFactory) {
        remoteActorSystems = remoteConfigurations
                .stream()
                .collect(Collectors.toMap(
                        RemoteActorSystemConfiguration::getClusterName,
                        remoteConfiguration -> new RemoteActorSystemInstance(
                                remoteConfiguration,
                                serializationFrameworks,
                                messageQueueFactoryFactory.create(
                                        remoteConfiguration.getClusterName()))));
    }

    @PostConstruct
    public void init() {
        remoteActorSystems.forEach((key, value) -> {
            try {
                value.init();
                logger.info(
                        "Added Remote ActorSystem [{}] with {} shards on Cluster [{}]",
                        value.getName(),
                        value.getNumberOfShards(),
                        key);
            } catch (Exception e) {
                logger.error(
                        "Exception while initializing Remote ActorSystem [{}]",
                        value.getName(),
                        e);
            }
        });
    }

    @PreDestroy
    public void destroy() {
        remoteActorSystems.forEach((s, remoteActorSystemInstance) -> remoteActorSystemInstance.destroy());
    }

    public RemoteActorSystemInstance get(String clusterName, String actorSystemName) {
        RemoteActorSystemInstance actorSystem = remoteActorSystems.get(clusterName);
        if (actorSystem != null && actorSystem.getName().equals(actorSystemName)) {
            return actorSystem;
        } else {
            return null;
        }
    }

    public RemoteActorSystemInstance get(String actorSystemName) {
        List<RemoteActorSystemInstance> instances = remoteActorSystems.values().stream()
                .filter(actorSystem -> actorSystem.getName().equals(actorSystemName))
                .collect(Collectors.toList());
        if (!instances.isEmpty()) {
            if (instances.size() > 1) {
                // cannot determine which one to use,
                throw new IllegalArgumentException(
                        "Found multiple matching Remote ActorSystems, please use ActorSystems.get"
                                + "(clusterName, actorSystemName");
            } else {
                return instances.get(0);
            }
        } else {
            return null;
        }
    }
}
