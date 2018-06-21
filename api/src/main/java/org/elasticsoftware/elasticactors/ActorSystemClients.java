package org.elasticsoftware.elasticactors;

public interface ActorSystemClients {
    ActorSystemClient getActorSystemClient(String clusterName, String actorSystemName);
}
