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

package org.elasticsoftware.elasticactors.test.reactivestreams;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.MethodActor;
import org.reactivestreams.Subscription;

/**
 * @author Joost van de Wijgerd
 */
@Actor
public final class TestSubscriber extends MethodActor {
    @Override
    public void postCreate(ActorRef creator) throws Exception {
        getSystem().actorFor("testPublisher").publisherOf(StreamedMessage.class).subscribe(asSubscriber());
    }

    @MessageHandler
    public void handle(StreamedMessage streamedMessage, ActorRef publisherRef) {
        System.out.println(getSelf().getActorId()+": "+streamedMessage.getKey());
        if(streamedMessage.getSequenceNumber() >= 10) {
            getSubscriptions().stream()
                    .filter(subscription -> subscription.getMessageName().equals(StreamedMessage.class.getName()) &&
                    subscription.getPublisherRef().equals(publisherRef)).forEach(Subscription::cancel);
            getSelf().tell(new StreamFinishedMessage());
        }
    }
}
