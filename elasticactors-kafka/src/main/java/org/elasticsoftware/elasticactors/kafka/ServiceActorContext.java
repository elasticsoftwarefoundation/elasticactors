package org.elasticsoftware.elasticactors.kafka;

import org.elasticsoftware.elasticactors.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public final class ServiceActorContext implements ActorContext {
    private final ActorRef serviceRef;
    private final ActorSystem actorSystem;

    public ServiceActorContext(ActorRef serviceRef, ActorSystem actorSystem) {
        this.serviceRef = serviceRef;
        this.actorSystem = actorSystem;
    }

    @Override
    public ActorRef getSelf() {
        return serviceRef;
    }

    @Override
    public <T extends ActorState> T getState(Class<T> stateClass) {
        return null;
    }

    @Override
    public void setState(ActorState state) {
        // noop
    }

    @Override
    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    @Override
    public Collection<PersistentSubscription> getSubscriptions() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, Set<ActorRef>> getSubscribers() {
        return Collections.emptyMap();
    }
}
