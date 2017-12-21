package org.elasticsoftware.elasticactors.kafka.cluster;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.cluster.ActorRefTools;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.kafka.KafkaActorNode;

public class KafkaActorRefTools extends ActorRefTools {
    private final KafkaInternalActorSystems actorSystems;

    public KafkaActorRefTools(KafkaInternalActorSystems actorSystems) {
        super(actorSystems);
        this.actorSystems = actorSystems;
    }

    @Override
    protected ActorRef handleNode(String[] components, String partitionAndActorId) {
        // actorId is actually <partition>/actorId
        String clusterName = components[0];
        String actorSystemName = components[1];
        int actorSeparator = partitionAndActorId.indexOf('/');
        int partition = Integer.parseInt(partitionAndActorId.substring(0, actorSeparator));
        String actorId = partitionAndActorId.substring(actorSeparator+1);
        InternalActorSystem actorSystem = actorSystems.get(actorSystemName);
        final KafkaActorNode node = (KafkaActorNode) actorSystem.getNode(components[3]);
        if(node != null) {
            return actorSystems.createTempActorRef(node, partition, actorId);
        } else {
            // this node is currently down, send a disconnected ref
            return new DisconnectedPartitionedActorNodeRef(clusterName, actorSystemName, components[3], partition, actorId);
        }
    }
}
