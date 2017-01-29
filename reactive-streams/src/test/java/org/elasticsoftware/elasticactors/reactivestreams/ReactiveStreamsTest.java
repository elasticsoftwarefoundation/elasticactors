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

import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.test.TestActorSystem;
import org.reactivestreams.*;
import org.reactivestreams.Subscription;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Joost van de Wijgerd
 */
public class ReactiveStreamsTest {
    @Test
    public void testSubscribing() throws Exception {
        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        actorSystem.actorOf("publisher", TestPublishingActor.class, new TestPublishingActorState());

        Publisher publisher = ReactiveStream.publisherOf(actorSystem, "publisher");

        Subscriber subscriber = new Subscriber() {
            Subscription subscription;
            int count = 0;

            @Override
            public void onSubscribe(Subscription s) {
                s.request(1000);
                subscription = s;
            }

            @Override
            public void onNext(Object o) {
                System.out.println("Received message: "+o.toString());
                if(count++ >= 10) {
                    subscription.cancel();
                }
                //countDownLatch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
                countDownLatch.countDown();
            }

            @Override
            public void onComplete() {
                System.out.println("onComplete");
                countDownLatch.countDown();
            }
        };

        publisher.subscribe(subscriber);

        countDownLatch.await();



    }
}
