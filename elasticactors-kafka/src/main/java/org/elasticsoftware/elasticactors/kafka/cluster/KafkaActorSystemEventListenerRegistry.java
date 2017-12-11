package org.elasticsoftware.elasticactors.kafka.cluster;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEvent;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerRegistry;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerService;

import java.io.IOException;

public class KafkaActorSystemEventListenerRegistry implements ActorSystemEventListenerService {
    @Override
    public void register(ActorRef receiver, ActorSystemEvent event, Object message) throws IOException {

    }

    @Override
    public void deregister(ActorRef receiver, ActorSystemEvent event) {

    }

    @Override
    public void generateEvents(ActorShard actorShard, ActorSystemEvent actorSystemEvent) {

    }
}
