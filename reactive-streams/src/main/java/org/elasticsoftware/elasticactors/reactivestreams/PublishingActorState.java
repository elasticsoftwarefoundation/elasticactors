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
import org.elasticsoftware.elasticactors.base.state.JacksonActorState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Joost van de Wijgerd
 */
public abstract class PublishingActorState<T> extends JacksonActorState<T> {

    private final Map<ActorRef, Subscription> subscriptions = new HashMap<>();

    public PublishingActorState(List<Subscription> subscriptions) {
        for (Subscription subscription : subscriptions) {
            this.subscriptions.put(subscription.getSubscriber(), subscription);
        }
    }

    public List<Subscription> getSubscriptions() {
        return new ArrayList<>(subscriptions.values());
    }

    boolean containsSubscription(ActorRef subscriber) {
        return this.subscriptions.containsKey(subscriber);
    }

    void addSubscription(ActorRef subscriber) {
        subscriptions.put(subscriber, new Subscription(subscriber, 0));
    }

    void removeSubscription(ActorRef subscriber) {
        subscriptions.remove(subscriber);
    }

    Subscription getSubscription(ActorRef subscriber) {
        return subscriptions.get(subscriber);
    }

}
