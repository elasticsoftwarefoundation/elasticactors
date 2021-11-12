/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.core.actors;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.TempActor;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.SubscribeMessage;
import org.elasticsoftware.elasticactors.reactivestreams.PersistentSubscriptionImpl;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.elasticsoftware.elasticactors.cluster.tasks.InternalActorContext.getAsProcessorContext;

/**
 * @author Joost van de Wijgerd
 */
@TempActor(stateClass = SubscriberState.class)
public final class SubscriberActor<T> extends TypedActor<T> implements Subscriber<T> {

    private final static Logger staticLogger = LoggerFactory.getLogger(SubscriberActor.class);

    @Override
    protected Logger initLogger() {
        return staticLogger;
    }

    @Override
    public void postCreate(ActorRef creator) throws Exception {
        SubscriberState state = getState(SubscriberState.class);
        // prepare the subscription
        getAsProcessorContext().addSubscription(new PersistentSubscriptionImpl(getSelf(),
                state.getPublisherRef(), state.getMessageName(), state.getSubscriber()));
        // and start the sequence
        state.getPublisherRef().tell(new SubscribeMessage(getSelf(), state.getMessageName()));
    }

    @Override
    public void onReceive(ActorRef sender, T message) throws Exception {
        // not used, everything is delegated to the Subscriber interface
    }

    @Override
    public Subscriber asSubscriber(@Nullable Class messageClass) {
        return this;
    }

    @Override
    public void onSubscribe(Subscription s) {
        getState(SubscriberState.class).getSubscriber().onSubscribe(s);
    }

    @Override
    public void onNext(T t) {
        getState(SubscriberState.class).getSubscriber().onNext(t);
    }

    @Override
    public void onError(Throwable t) {
        getState(SubscriberState.class).getSubscriber().onError(t);
        try {
            getSystem().stop(getSelf());
        } catch (Exception e) {
           // ignore
        }
    }

    @Override
    public void onComplete() {
        getState(SubscriberState.class).getSubscriber().onComplete();
        try {
            getSystem().stop(getSelf());
        } catch (Exception e) {
            // ignore
        }
    }
}
