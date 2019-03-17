package org.elasticsoftware.elasticactors.client.common;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerRegistry;
import org.elasticsoftware.elasticactors.scheduler.Scheduler;

import javax.annotation.Nullable;
import java.util.Collection;

public final class ActorSystemWrapper implements ActorSystem {
    private final ActorSystemClient delegate;

    public ActorSystemWrapper(ActorSystemClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass) throws Exception {
        throw new UnsupportedOperationException("You cannot create Actors from an ActorSystemClient");
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName) throws Exception {
        throw new UnsupportedOperationException("You cannot create Actors from an ActorSystemClient");
    }

    @Override
    public <T> ActorRef actorOf(String actorId, Class<T> actorClass, ActorState initialState) throws Exception {
        throw new UnsupportedOperationException("You cannot create Actors from an ActorSystemClient");
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName, ActorState initialState) throws Exception {
        throw new UnsupportedOperationException("You cannot create Actors from an ActorSystemClient");
    }

    @Override
    public ActorRef actorOf(String actorId, String actorClassName, ActorState initialState, ActorRef creatorRef) throws Exception {
        throw new UnsupportedOperationException("You cannot create Actors from an ActorSystemClient");
    }

    @Override
    public <T extends ElasticActor> ActorRef tempActorOf(Class<T> actorClass, @Nullable ActorState initialState) throws Exception {
        return delegate.tempActorOf(actorClass, initialState);
    }

    @Override
    public ActorRef actorFor(String actorId) {
        return delegate.actorFor(actorId);
    }

    @Override
    public ActorRef serviceActorFor(String actorId) {
        throw new UnsupportedOperationException("You cannot reference Service Actors from an ActorSystemClient");
    }

    @Override
    public ActorRef serviceActorFor(String nodeId, String actorId) {
        throw new UnsupportedOperationException("You cannot reference Service Actors from an ActorSystemClient");
    }

    @Override
    public Scheduler getScheduler() {
        throw new UnsupportedOperationException("The Scheduler is not available from an ActorSystemClient");
    }

    @Override
    public ActorSystems getParent() {
        throw new UnsupportedOperationException("ActorSystem is not available from an ActorSystemClient");
    }

    @Override
    public void stop(ActorRef actorRef) throws Exception {
        delegate.stop(actorRef);
    }

    @Override
    public ActorSystemConfiguration getConfiguration() {
        throw new UnsupportedOperationException("Access to the ActorSystemConfiguration is not possible from an ActorSystemClient");
    }

    @Override
    public ActorSystemEventListenerRegistry getEventListenerRegistry() {
        throw new UnsupportedOperationException("Access to the ActorSystemEventListenerRegistry is not possible from an ActorSystemClient");
    }

    @Override
    public ActorRefGroup groupOf(Collection<ActorRef> members) throws IllegalArgumentException {
        throw new UnsupportedOperationException("ActorRefGroup functionality is currently not supported");
    }
}
