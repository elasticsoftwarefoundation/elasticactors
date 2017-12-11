package org.elasticsoftware.elasticactors.kafka.utils;

import org.elasticsoftware.elasticactors.NodeKey;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;

import static java.lang.String.format;

public final class TopicNamesHelper {
    private static final String PERSISTENT_ACTORS = "ElasticActors_PersistentActors-%s-%s";
    private static final String SCHEDULED_MESSAGES = "ElasticActors_ScheduledMessages-%s-%s";
    private static final String ACTORSYSTEM_EVENT_LISTENERS = "ElasticActors_ActorSystemEventListeners-%s-%s";
    private static final String MESSAGES = "ElasticActors_Messages-%s-%s";
    private static final String NODE_MESSAGES = "ElasticActors_Messages-%s-%s-nodes-%s";

    private TopicNamesHelper() {

    }

    public static String getPersistentActorsTopic(InternalActorSystem internalActorSystem) {
        return format(PERSISTENT_ACTORS, internalActorSystem.getParent().getClusterName(), internalActorSystem.getName());
    }

    public static String getScheduledMessagesTopic(InternalActorSystem internalActorSystem) {
        return format(SCHEDULED_MESSAGES, internalActorSystem.getParent().getClusterName(), internalActorSystem.getName());
    }

    public static String getActorsystemEventListenersTopic(InternalActorSystem internalActorSystem) {
        return format(ACTORSYSTEM_EVENT_LISTENERS, internalActorSystem.getParent().getClusterName(), internalActorSystem.getName());
    }

    public static String getMessagesTopic(InternalActorSystem internalActorSystem) {
        return format(MESSAGES, internalActorSystem.getParent().getClusterName(), internalActorSystem.getName());
    }

    public static String getNodeMessagesTopic(InternalActorSystem internalActorSystem, NodeKey node) {
        return format(NODE_MESSAGES, internalActorSystem.getParent().getClusterName(), internalActorSystem.getName(), node.getNodeId());
    }

}
