package org.elasticsoftware.elasticactors.cluster;

import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.InternalActorSystemConfiguration;
import org.elasticsoftware.elasticactors.RemoteActorSystemConfiguration;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactoryFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class RemoteActorSystems {
    private static final Logger logger = Logger.getLogger(RemoteActorSystems.class);
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
            remoteActorSystemInstance.destroy();
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
