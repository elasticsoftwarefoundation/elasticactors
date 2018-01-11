package org.elasticsoftware.elasticactors.kafka.cluster;


import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.state.PersistentActor;

@FunctionalInterface
public interface ActorLifecycleFunction {
    Boolean apply(InternalActorSystem actorSystem, PersistentActor persistentActor, ElasticActor receiver, ActorRef receiverRef, InternalMessage internalMessage);
}
