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

package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.base.actors.ActorDelegate;
import org.elasticsoftware.elasticactors.base.actors.ReplyActor;
import org.elasticsoftware.elasticactors.test.common.CurrentActorName;
import org.elasticsoftware.elasticactors.test.common.ExclusiveManagedNameActor;
import org.elasticsoftware.elasticactors.test.common.GetActorName;
import org.elasticsoftware.elasticactors.test.common.ManagedNameActor;
import org.elasticsoftware.elasticactors.test.common.NameActorState;
import org.elasticsoftware.elasticactors.test.common.SetActorName;
import org.elasticsoftware.elasticactors.test.messaging.LocalMessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ManagedActorTest {

    private static final Logger logger = LoggerFactory.getLogger(ManagedActorTest.class);

    @Test
    public void testGetActorName() throws Exception {

        logger.info("Starting managedActorTest");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef managedActor = actorSystem.actorFor(ManagedNameActor.ACTOR_ID_0);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .onReceive(
                        CurrentActorName.class,
                        m -> {
                            logger.info("Got current actor name '{}'", m.getCurrentName());
                            assertEquals(m.getCurrentName(), ManagedNameActor.ACTOR_ID_0);
                        })
                .postReceive(countDownLatch::countDown)
                .build());

        managedActor.tell(new GetActorName(), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void testSetActorName() throws Exception {

        logger.info("Starting managedActorTest");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef managedActor = actorSystem.actorFor(ManagedNameActor.ACTOR_ID_1);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .onReceive(
                        CurrentActorName.class,
                        m -> {
                            logger.info("Got current actor name '{}'", m.getCurrentName());
                            assertEquals(m.getCurrentName(), ManagedNameActor.ACTOR_ID_1);
                        })
                .postReceive(countDownLatch::countDown)
                .build());

        managedActor.tell(new SetActorName("New name"), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void testGetActorNameAfterSet() throws Exception {

        logger.info("Starting managedActorTest");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef managedActor = actorSystem.actorFor(ManagedNameActor.ACTOR_ID_2);
        managedActor.tell(new SetActorName("Test actor name"), null);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .onReceive(
                        CurrentActorName.class,
                        m -> {
                            logger.info("Got current actor name '{}'", m.getCurrentName());
                            assertEquals(m.getCurrentName(), "Test actor name");
                        })
                .postReceive(countDownLatch::countDown)
                .build());

        managedActor.tell(new GetActorName(), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void testCreateOtherActorsOfTheType() throws Exception {

        logger.info("Starting managedActorTest");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef anotherActor = actorSystem.actorOf(
                "someId",
                ManagedNameActor.class,
                new NameActorState("someName"));

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .onReceive(
                        CurrentActorName.class,
                        m -> {
                            logger.info("Got current actor name '{}'", m.getCurrentName());
                            assertEquals(m.getCurrentName(), "someName");
                        })
                .postReceive(countDownLatch::countDown)
                .build());

        anotherActor.tell(new GetActorName(), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void exclusive_testGetActorName() throws Exception {

        logger.info("Starting managedActorTest");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef managedActor = actorSystem.actorFor(ExclusiveManagedNameActor.ACTOR_ID_0);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .onReceive(
                        CurrentActorName.class,
                        m -> {
                            logger.info("Got current actor name '{}'", m.getCurrentName());
                            assertEquals(m.getCurrentName(), ExclusiveManagedNameActor.ACTOR_ID_0);
                        })
                .postReceive(countDownLatch::countDown)
                .build());

        managedActor.tell(new GetActorName(), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void exclusive_testSetActorName() throws Exception {

        logger.info("Starting managedActorTest");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef managedActor = actorSystem.actorFor(ExclusiveManagedNameActor.ACTOR_ID_1);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .onReceive(
                        CurrentActorName.class,
                        m -> {
                            logger.info("Got current actor name '{}'", m.getCurrentName());
                            assertEquals(m.getCurrentName(), ExclusiveManagedNameActor.ACTOR_ID_1);
                        })
                .postReceive(countDownLatch::countDown)
                .build());

        managedActor.tell(new SetActorName("New name"), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void exclusive_testGetActorNameAfterSet() throws Exception {

        logger.info("Starting managedActorTest");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef managedActor = actorSystem.actorFor(ExclusiveManagedNameActor.ACTOR_ID_2);
        managedActor.tell(new SetActorName("Test actor name"), null);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .onReceive(
                        CurrentActorName.class,
                        m -> {
                            logger.info("Got current actor name '{}'", m.getCurrentName());
                            assertEquals(m.getCurrentName(), "Test actor name");
                        })
                .postReceive(countDownLatch::countDown)
                .build());

        managedActor.tell(new GetActorName(), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void exclusive_testThrowExceptionOnActorCreate() throws Exception {

        logger.info("Starting managedActorTest");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        actorSystem.actorOf(
                "someId",
                ExclusiveManagedNameActor.class,
                new NameActorState("someName"));

        testActorSystem.destroy();
    }

}
