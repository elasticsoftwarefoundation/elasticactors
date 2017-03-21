/*
 * Copyright 2013 - 2017 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.core.actors;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.CancelMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.CompletedMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.RequestMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.SubscriptionMessage;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.BiFunction;

/**
 * @author Joost van de Wijgerd
 */
public final class SubscriberDelegate<T> extends ActorDelegate<Object> implements Subscription {
    private volatile ActorRef delegateRef;
    private final ActorRef publisherRef;
    private final String messageName;
    private final Subscriber<? super T> subscriber;
    private BiFunction<ActorRef, Object, BiFunction> handler = this::handleSubscription;

    public SubscriberDelegate(ActorRef publisherRef, String messageName, Subscriber<? super T> subscriber) {
        super(false);
        this.publisherRef = publisherRef;
        this.messageName = messageName;
        this.subscriber = subscriber;
    }

    @Override
    public void postCreate(ActorRef creator) throws Exception {
        // we need to store this to be accessed by other threads
        this.delegateRef = getSelf();
    }

    @Override
    public void onReceive(ActorRef publisherRef, Object message) throws Exception {
        if(handler != null) {
            handler = handler.apply(publisherRef, message);
        }
    }

    private BiFunction<ActorRef, Object, BiFunction> handleSubscription(ActorRef publisherRef, Object message) {
        // This will be called on the ActorThread of the TempActor
        if(message instanceof SubscriptionMessage && ((SubscriptionMessage)message).getMessageName().equals(messageName)) {
            // delegate handling to the subscriber
            subscriber.onSubscribe(this);
            // I need to stay active until completed by the publisher
            return this::handleMessages;
        } else {
            // @todo: come up with a proper exception here
            subscriber.onError(new IllegalStateException("Did not receive correct Subscription from Publisher"));
            // remove self
            try {
                getSystem().stop(getSelf());
            } catch (Exception e) {
                // ignore
            }
            delegateRef = null;
            return null;
        }
    }

    private BiFunction<ActorRef, Object, BiFunction> handleMessages(ActorRef publisherRef, Object message) {
        if(message instanceof CompletedMessage) {
            subscriber.onComplete();
            // remove self
            try {
                getSystem().stop(getSelf());
            } catch (Exception e) {
                // ignore
            }
            delegateRef = null;
            return null;
        } else {
            subscriber.onNext((T) message);
            return this::handleMessages;
        }
    }

    @Override
    public void onUndeliverable(ActorRef receiver, Object message) throws Exception {
        // the actor doesn't exist
        // @todo: come up with proper exception here
        subscriber.onError(new RuntimeException("The Publisher actor does not exist"));
    }

    @Override
    public void request(long n) {
        if(delegateRef != null) {
            try {
                publisherRef.tell(new RequestMessage(n), delegateRef);
            } catch (Exception e) {
                // @todo: the subscription is now invalid
                subscriber.onError(e);
            }
        }
    }

    @Override
    public void cancel() {
        if(delegateRef != null) {
            publisherRef.tell(new CancelMessage(delegateRef, messageName), delegateRef);
        }
    }
}
