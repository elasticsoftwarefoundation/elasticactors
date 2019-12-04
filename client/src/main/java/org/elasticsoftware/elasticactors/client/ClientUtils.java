package org.elasticsoftware.elasticactors.client;

import org.elasticsoftware.elasticactors.ShardKey;

public final class ClientUtils {

    /**
     * A utility method for creating an exchange on a AMQP implementation of a MessageQueue
     */
    public static String createExchangeName(String clusterName) {
        return String.format("ea.%s", clusterName);
    }

    /**
     * A utility method for creating queues/routing keys on a AMQP implementation of a MessageQueue
     */
    public static String createQueueName(String clusterName, ShardKey key) {
        return String.format(
                "%s/%s/shards/%d",
                clusterName,
                key.getActorSystemName(),
                key.getShardId());
    }

}
