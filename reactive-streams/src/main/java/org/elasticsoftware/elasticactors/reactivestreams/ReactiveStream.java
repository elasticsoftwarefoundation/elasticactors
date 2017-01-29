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
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.reactivestreams.protocol.Subscribe;
import org.reactivestreams.Publisher;

/**
 * @author Joost van de Wijgerd
 */
public final class ReactiveStream {
    public static <T> Publisher<T> publisherOf(ActorSystem actorSystem, String actorId) {
        return s -> {
            try {
                ActorRef publisher = actorSystem.actorFor(actorId);
                ActorRef subscriber = actorSystem.tempActorOf(SubscriberActor.class, new SubscriberActorDelegate<>(s, publisher));
                publisher.tell(new Subscribe(), subscriber);
            } catch(Exception e) {
                s.onError(e);
            }
        };
    }
    public static <T> Publisher<T> publisherOf(ActorSystem actorSystem, String actorId, Object subscriptionMessage) {
        return s -> {
            try {
                ActorRef publisher = actorSystem.actorFor(actorId);
                ActorRef subscriber = actorSystem.tempActorOf(SubscriberActor.class, new SubscriberActorDelegate<>(s, publisher));
                publisher.tell(subscriptionMessage, subscriber);
            } catch(Exception e) {
                s.onError(e);
            }
        };
    }
}
