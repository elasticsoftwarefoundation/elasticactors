/*
 * Copyright 2013 - 2024 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.kafka.cluster;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.PersistentSubscription;
import org.elasticsoftware.elasticactors.SubscriberContext;
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
