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
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.reactivestreams.protocol.*;

/**
 * @author Joost van de Wijgerd
 */
public abstract class PublishingActor extends MethodActor {
    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
        if(message instanceof ProtocolMessage) {
            updateInternalState(sender, (ProtocolMessage) message);
        } else {
            super.onReceive(sender, message);
        }
    }

    private void updateInternalState(ActorRef subscriber, ProtocolMessage message) {
        PublishingActorState state = getState(PublishingActorState.class);
        if(message instanceof Subscribe) {
            if(!state.containsSubscription(subscriber)) {
                state.addSubscription(subscriber);
                subscriber.tell(new Subscribed(), getSelf());
            }
        } else if(message instanceof Cancel) {
            state.removeSubscription(subscriber);
            subscriber.tell(new Completed(), getSelf());
        } else if(message instanceof Request) {
            Subscription subscription = state.getSubscription(subscriber);
            if(subscription != null) {
                Request request = (Request) message;
                subscription.incrementDemand(request.getNumber());
            }
        }
    }

    protected final void publish(Object message) {
        PublishingActorState state = getState(PublishingActorState.class);
        for (Object subscription : state.getSubscriptions()) {
            ((Subscription)subscription).publish(message);
        }
    }
}
