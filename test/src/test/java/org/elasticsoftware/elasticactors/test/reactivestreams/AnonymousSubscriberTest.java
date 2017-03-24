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

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.test.TestActorSystem;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author Joost van de Wijgerd
 */
public class AnonymousSubscriberTest {
    @Test
    public void testStream() throws Exception {
        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();

        ActorRef testPublisher = actorSystem.actorOf("testPublisher", TestPublisher.class);

        final CountDownLatch waitLatch = new CountDownLatch(1);

        testPublisher.publisherOf(StreamedMessage.class).subscribe(new Subscriber<StreamedMessage>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(StreamedMessage streamedMessage) {
                System.out.println(streamedMessage.getKey());
                if(streamedMessage.getSequenceNumber() >= 10) {
                    subscription.cancel();
                }
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace(System.err);
                waitLatch.countDown();
            }

            @Override
            public void onComplete() {
                waitLatch.countDown();
            }
        });

        waitLatch.await();

        testActorSystem.destroy();
    }

    @Test
    public void testMultipleSubscribers() throws Exception {
        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();

        ActorRef testPublisher = actorSystem.actorOf("testPublisher", TestPublisher.class);

        final CountDownLatch waitLatch = new CountDownLatch(2);

        testPublisher.publisherOf(StreamedMessage.class).subscribe(new Subscriber<StreamedMessage>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(StreamedMessage streamedMessage) {
                System.out.println("Subscriber 1: "+streamedMessage.getKey());
                if(streamedMessage.getSequenceNumber() >= 10) {
                    subscription.cancel();
                }
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace(System.err);
                waitLatch.countDown();
            }

            @Override
            public void onComplete() {
                waitLatch.countDown();
            }
        });

        testPublisher.publisherOf(StreamedMessage.class).subscribe(new Subscriber<StreamedMessage>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                subscription = s;
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(StreamedMessage streamedMessage) {
                System.out.println("Subscriber 2: "+streamedMessage.getKey());
                if(streamedMessage.getSequenceNumber() >= 10) {
                    subscription.cancel();
                }
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace(System.err);
                waitLatch.countDown();
            }

            @Override
            public void onComplete() {
                waitLatch.countDown();
            }
        });

        waitLatch.await();

        testActorSystem.destroy();
    }
}
