/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.test.common;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.base.actors.ActorDelegate;
import org.elasticsoftware.elasticactors.base.actors.ReplyActor;
import org.elasticsoftware.elasticactors.base.state.StringState;
import org.elasticsoftware.elasticactors.test.TestActorSystem;
import org.elasticsoftware.elasticactors.test.messaging.LocalMessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

public class CacheExpirationTest {

    private final static Logger logger = LoggerFactory.getLogger(CacheExpirationTest.class);

    @Test
    public void testWontExpire() throws Exception {
        System.setProperty("ea.nodeCache.expirationCheckPeriod", "10000");

        logger.info("Starting testGreeting");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef greeter =
            actorSystem.actorOf("greeter", GreetingActor.class, new StringState("Hello World"));

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
            .deleteAfterReceive(false)
            .timeout(2000L)
            .onReceive(ScheduledGreeting.class, () -> logger.info("Got Scheduled Greeting"))
            .onReceive(Greeting.class, m -> logger.info("Got Greeting from {}", m.getWho()))
            .orElse(ActorDelegate.MessageConsumer.noop())
            .postReceive(countDownLatch::countDown)
            .build());

        greeter.tell(new Greeting("Joost van de Wijgerd", false), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void testExpireOnReceive() throws Exception {
        System.setProperty("ea.nodeCache.expirationCheckPeriod", "10000");

        logger.info("Starting testGreeting");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef greeter =
            actorSystem.actorOf("greeter", GreetingActor.class, new StringState("Hello World"));

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
            .deleteAfterReceive(false)
            .timeout(1_500L)
            .onReceive(ScheduledGreeting.class, () -> logger.info("Got Scheduled Greeting"))
            .onReceive(Greeting.class, m -> logger.info("Got Greeting from {}", m.getWho()))
            .orElse(ActorDelegate.MessageConsumer.noop())
            .postReceive(countDownLatch::countDown)
            .build());

        Thread.sleep(1_000L);

        greeter.tell(new Greeting("Joost van de Wijgerd", false), replyActor);

        assertFalse(countDownLatch.await(5, TimeUnit.SECONDS));
        assertEquals(countDownLatch.getCount(), 1);
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void testExpireAsync() throws Exception {
        System.setProperty("ea.nodeCache.expirationCheckPeriod", "500");

        logger.info("Starting testGreeting");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef greeter =
            actorSystem.actorOf("greeter", GreetingActor.class, new StringState("Hello World"));

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
            .deleteAfterReceive(false)
            .timeout(1_000L)
            .onReceive(ScheduledGreeting.class, () -> logger.info("Got Scheduled Greeting"))
            .onReceive(Greeting.class, m -> logger.info("Got Greeting from {}", m.getWho()))
            .orElse(ActorDelegate.MessageConsumer.noop())
            .postReceive(countDownLatch::countDown)
            .build());

        Thread.sleep(2_500L);

        greeter.tell(new Greeting("Joost van de Wijgerd", false), replyActor);

        assertFalse(countDownLatch.await(5, TimeUnit.SECONDS));
        assertEquals(countDownLatch.getCount(), 2);
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

}
