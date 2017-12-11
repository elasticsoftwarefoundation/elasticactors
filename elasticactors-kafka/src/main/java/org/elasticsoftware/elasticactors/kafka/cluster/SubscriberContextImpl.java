package org.elasticsoftware.elasticactors.kafka.cluster;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.state.PersistentActor;

public final class SubscriberContextImpl implements SubscriberContext {
    private final PersistentActor persistentActor;
    private final ActorRef publisher;
    private final ActorSystem actorSystem;
    private final PersistentSubscription subscription;

    public SubscriberContextImpl(PersistentActor persistentActor,
                                 ActorRef publisher,
                                 ActorSystem actorSystem,
                                 PersistentSubscription subscription) {
        this.persistentActor = persistentActor;
        this.publisher = publisher;
        this.actorSystem = actorSystem;
        this.subscription = subscription;
    }

    @Override
    public ActorRef getSelf() {
        return persistentActor.getSelf();
    }

    @Override
    public ActorRef getPublisher() {
        return publisher;
    }

    @Override
    public <T extends ActorState> T getState(Class<T> stateClass) {
        return stateClass.cast(persistentActor.getState());
    }

    @Override
    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    @Override
    public PersistentSubscription getSubscription() {
        return subscription;
    }
}
