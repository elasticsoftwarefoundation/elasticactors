/*
 * Copyright 2013 - 2017 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.InternalActorSystemConfiguration;
import org.elasticsoftware.elasticactors.RemoteActorSystemConfiguration;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactoryFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class RemoteActorSystems {
    private static final Logger logger = LogManager.getLogger(RemoteActorSystems.class);
    private final Map<String,RemoteActorSystemInstance> remoteActorSystems = new HashMap<>();
    private final InternalActorSystemConfiguration configuration;
    private final InternalActorSystems cluster;
    private MessageQueueFactoryFactory remoteActorSystemMessageQueueFactoryFactory;

    public RemoteActorSystems(InternalActorSystemConfiguration configuration, InternalActorSystems cluster) {
        this.configuration = configuration;
        this.cluster = cluster;
    }


    public void init() throws Exception {
        // initialize the remote systems
        for (RemoteActorSystemConfiguration remoteConfiguration : configuration.getRemoteConfigurations()) {
            RemoteActorSystemInstance remoteInstance = new RemoteActorSystemInstance(remoteConfiguration,cluster,remoteActorSystemMessageQueueFactoryFactory.create(remoteConfiguration.getClusterName()));
            remoteActorSystems.put(remoteConfiguration.getClusterName(),remoteInstance);
            remoteInstance.init();
            logger.info(format("Added Remote ActorSystem [%s] with %d shards on Cluster [%s]",remoteInstance.getName(),remoteConfiguration.getNumberOfShards(),remoteConfiguration.getClusterName()));
        }
    }

    @PreDestroy
    public void destroy() {
        for (RemoteActorSystemInstance remoteActorSystemInstance : remoteActorSystems.values()) {
            try {
                remoteActorSystemInstance.destroy();
            } catch(Exception e) {
                // ignore
            }
        }
    }

    @Autowired
    public void setRemoteActorSystemMessageQueueFactoryFactory(@Qualifier("remoteActorSystemMessageQueueFactoryFactory") MessageQueueFactoryFactory remoteActorSystemMessageQueueFactoryFactory) {
        this.remoteActorSystemMessageQueueFactoryFactory = remoteActorSystemMessageQueueFactoryFactory;
    }

    public ActorSystem get(String clusterName,String actorSystemName) {
        RemoteActorSystemInstance actorSystem = remoteActorSystems.get(clusterName);
        if(actorSystem != null && actorSystem.getName().equals(actorSystemName)) {
            return actorSystem;
        } else {
            return null;
        }
    }
}
