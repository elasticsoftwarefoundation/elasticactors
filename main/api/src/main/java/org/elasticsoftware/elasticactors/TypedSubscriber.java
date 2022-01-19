/*
 *   Copyright 2013 - 2022 The Original Authors
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

package org.elasticsoftware.elasticactors;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * @author Joost van de Wijgerd
 */
public abstract class TypedSubscriber<T> implements Subscriber<T> {
    @Override
    public void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);
    }

    @Override
    public void onError(Throwable error) {
        // swallow the error by default
    }

    @Override
    public void onComplete() {
        // do nothing by default
    }

    protected final ActorRef getSelf() {
        return SubscriberContextHolder.getSelf();
    }

    protected final ActorRef getPublisher() {
        return SubscriberContextHolder.getPublisher();
    }

    protected final PersistentSubscription getSubscription() {
        return SubscriberContextHolder.getSubscription();
    }

    protected <C extends ActorState> C getState(Class<C> stateClass) {
        return ActorContextHolder.getState(stateClass);
    }

    protected final ActorSystem getSystem() {
        return ActorContextHolder.getSystem();
    }
}
