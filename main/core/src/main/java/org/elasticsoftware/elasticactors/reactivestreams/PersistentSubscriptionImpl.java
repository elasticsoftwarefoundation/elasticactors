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

package org.elasticsoftware.elasticactors.reactivestreams;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.CancelMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.RequestMessage;
import org.reactivestreams.Subscriber;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentSubscriptionImpl implements InternalPersistentSubscription {
    private final ActorRef subscriberRef;
    private final ActorRef publisherRef;
    private final String messageName;
    private final AtomicBoolean cancelled;
    private final transient Subscriber subscriber;

    public PersistentSubscriptionImpl(ActorRef subscriberRef, ActorRef publisherRef, String messageName) {
        this(subscriberRef, publisherRef, messageName, false, null);
    }

    public PersistentSubscriptionImpl(ActorRef subscriberRef, ActorRef publisherRef, String messageName, Subscriber subscriber) {
        this(subscriberRef, publisherRef, messageName, false, subscriber);

    }

    public PersistentSubscriptionImpl(ActorRef subscriberRef, ActorRef publisherRef, String messageName, boolean cancelled) {
        this(subscriberRef, publisherRef, messageName, cancelled, null);
    }

    public PersistentSubscriptionImpl(ActorRef subscriberRef, ActorRef publisherRef, String messageName, boolean cancelled, Subscriber subscriber) {
        this.subscriberRef = subscriberRef;
        this.publisherRef = publisherRef;
        this.messageName = messageName;
        this.cancelled = new AtomicBoolean(cancelled);
        this.subscriber = subscriber;
    }

    @Override
    public ActorRef getPublisherRef() {
        return publisherRef;
    }

    @Override
    public String getMessageName() {
        return messageName;
    }

    @Override
    public boolean isCancelled() {
        return cancelled.get();
    }

    @Override
    public void request(long n) {
        if(!cancelled.get()) {
            publisherRef.tell(new RequestMessage(messageName, n), subscriberRef);
        }
    }

    @Override
    public void cancel() {
        if(cancelled.compareAndSet(false, true)) {
            publisherRef.tell(new CancelMessage(subscriberRef, messageName), subscriberRef);
        }
    }

    @Override
    public Subscriber getSubscriber() {
        return subscriber;
    }
}
