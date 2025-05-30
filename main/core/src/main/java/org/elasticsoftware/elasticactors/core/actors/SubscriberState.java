/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.core.actors;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.serialization.NoopSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.reactivestreams.Subscriber;

/**
 * @author Joost van de Wijgerd
 */
public final class SubscriberState<T> implements ActorState<SubscriberState> {
    private final Subscriber<T> subscriber;
    private final ActorRef publisherRef;
    private final String messageName;

    public SubscriberState(Subscriber<T> subscriber,
                           ActorRef publisherRef,
                           String messageName) {
        this.subscriber = subscriber;
        this.publisherRef = publisherRef;
        this.messageName = messageName;
    }

    public Subscriber<T> getSubscriber() {
        return subscriber;
    }

    public ActorRef getPublisherRef() {
        return publisherRef;
    }

    public String getMessageName() {
        return messageName;
    }

    @Override
    public SubscriberState getBody() {
        return this;
    }

    @Override
    public Class<? extends SerializationFramework> getSerializationFramework() {
        return NoopSerializationFramework.class;
    }
}
