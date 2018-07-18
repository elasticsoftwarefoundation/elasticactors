package org.elasticsoftware.elasticactors.runtime;

import org.elasticsoftware.elasticactors.ActorSystemClient;
import org.elasticsoftware.elasticactors.ActorSystemClients;

public class ElasticActorsClient implements ActorSystemClients {
    @Override
    public ActorSystemClient getActorSystemClient(String clusterName, String actorSystemName) {
        return null;
    }
}
