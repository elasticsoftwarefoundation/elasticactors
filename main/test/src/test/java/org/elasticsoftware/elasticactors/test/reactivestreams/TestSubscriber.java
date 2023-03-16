/*
 * Copyright 2013 - 2023 The Original Authors
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

package org.elasticsoftware.elasticactors.test.reactivestreams;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.PublisherNotFoundException;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * @author Joost van de Wijgerd
 */
@Actor
public final class TestSubscriber extends MethodActor {

    @Override
    public void postCreate(ActorRef creator) throws Exception {
        getSystem().actorFor("testPublisher").publisherOf(StreamedMessage.class).subscribe(asSubscriber(StreamedMessage.class));
    }

    @MessageHandler
    public void handle(StreamedMessage streamedMessage, ActorRef publisherRef) {
        logger.info("{}: {}", getSelf().getActorId(), streamedMessage.getKey());
        if(streamedMessage.getSequenceNumber() >= 10) {
            getSubscriptions().stream()
                    .filter(subscription -> subscription.getMessageName().equals(StreamedMessage.class.getName()) &&
                    subscription.getPublisherRef().equals(publisherRef)).forEach(Subscription::cancel);
            getSelf().tell(new StreamFinishedMessage());
        }
    }

    @Override
    public Subscriber asSubscriber(Class messageClass) {
        return new DefaultSubscriber() {
            @Override
            public void onError(Throwable error) {
                if(error instanceof PublisherNotFoundException) {
                    try {
                        getSystem().actorOf("testPublisher", TestPublisher.class).publisherOf(StreamedMessage.class)
                                .subscribe(this);
                    } catch(Exception e) {

                    }
                }
            }
        };
    }
}
