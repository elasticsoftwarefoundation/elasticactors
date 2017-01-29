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

package org.elasticsoftware.elasticactors.reactivestreams;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.reactivestreams.protocol.Cancel;
import org.elasticsoftware.elasticactors.reactivestreams.protocol.Completed;
import org.elasticsoftware.elasticactors.reactivestreams.protocol.Request;
import org.elasticsoftware.elasticactors.reactivestreams.protocol.Subscribed;
import org.elasticsoftware.elasticactors.serialization.NoopSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * @author Joost van de Wijgerd
 */
public class SubscriberActorDelegate<T> extends TypedActor<Object> implements ActorState<SubscriberActorDelegate<T>> {
    private final Subscriber<? super T> delegate;
    private final ActorRef publisher;

    public SubscriberActorDelegate(Subscriber<? super T> delegate, ActorRef publisher) {
        this.delegate = delegate;
        this.publisher = publisher;
    }

    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
        if (message instanceof Subscribed) {
            delegate.onSubscribe(new InternalSubscription(getSelf()));
        } else if (message instanceof Completed) {
            delegate.onComplete();
        } else if (message instanceof Throwable) {
            delegate.onError((Throwable) message);
        } else {
            delegate.onNext((T) message);
        }
    }

    @Override
    public void onUndeliverable(ActorRef receiver, Object message) throws Exception {
        delegate.onError(new NoSuchPublisherException());
    }

    @Override
    public SubscriberActorDelegate<T> getBody() {
        return this;
    }

    @Override
    public Class<? extends SerializationFramework> getSerializationFramework() {
        return NoopSerializationFramework.class;
    }

    private final class InternalSubscription implements Subscription {
        private final ActorRef self;

        public InternalSubscription(ActorRef self) {
            this.self = self;
        }

        @Override
        public void request(long n) {
            publisher.tell(new Request(n), self);
        }

        @Override
        public void cancel() {
            publisher.tell(new Cancel(), self);
        }
    }

}
