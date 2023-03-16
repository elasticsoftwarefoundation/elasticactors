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

package org.elasticsoftware.elasticactors.test.common;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.base.actors.ActorDelegate;
import org.elasticsoftware.elasticactors.base.actors.ActorDelegate.MessageConsumer;
import org.elasticsoftware.elasticactors.base.actors.ReplyActor;
import org.elasticsoftware.elasticactors.base.state.StringState;
import org.elasticsoftware.elasticactors.test.TestActorSystem;
import org.elasticsoftware.elasticactors.test.messaging.LocalMessageQueue;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsoftware.elasticactors.base.actors.ActorDelegate.Builder.stopActor;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;
import static org.elasticsoftware.elasticactors.tracing.TracingUtils.shorten;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * @author Joost van de Wijgerd
 */
public class GreetingTest {

    private static final ThreadLocal<MessagingScope> testScope = new ThreadLocal<>();
    public static final TraceContext TEST_TRACE = new TraceContext();

    @BeforeMethod
    public void addExternalCreatorData(Method method) {
        testScope.set(getManager().enter(
                TEST_TRACE,
                new CreationContext(
                        this.getClass().getSimpleName(),
                        this.getClass(),
                        method)));
    }

    @AfterMethod
    public void removeExternalCreatorData() {
        testScope.get().close();
        assertNull(getManager().currentScope());
        testScope.remove();
    }

    private static final Logger logger = LoggerFactory.getLogger(GreetingTest.class);

    @Test
    public void testGreeting() throws Exception {

        logger.info("Starting testGreeting");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef greeter = actorSystem.actorOf("greeter",GreetingActor.class,new StringState("Hello World"));

        //ScheduledMessageRef messageRef = actorSystem.getScheduler().scheduleOnce(null,new Greeting("Delayed Message"),greeter,2, TimeUnit.SECONDS);
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .deleteAfterReceive(false)
                .onReceive(ScheduledGreeting.class, () -> logger.info("Got Scheduled Greeting"))
                .onReceive(Greeting.class, m -> logger.info("Got Greeting from {}", m.getWho()))
                .orElse(MessageConsumer.noop())
                .postReceive(countDownLatch::countDown)
                .build());

        greeter.tell(new Greeting("Joost van de Wijgerd"), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void testGreeting_messageContext() throws Exception {

        logger.info("Starting testGreeting");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef greeter = actorSystem.actorOf("greeter",GreetingActor.class,new StringState("Hello World"));

        //ScheduledMessageRef messageRef = actorSystem.getScheduler().scheduleOnce(null,new Greeting("Delayed Message"),greeter,2, TimeUnit.SECONDS);
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .deleteAfterReceive(false)
                .onReceive(ScheduledGreeting.class, () -> {
                    logger.info("Got Scheduled Greeting");
                    if (getManager().isTracingEnabled()) {
                        MessagingScope scope = getManager().currentScope();
                        assertNotNull(scope);
                        assertNull(scope.getMethod());
                        CreationContext creationContext = scope.getCreationContext();
                        assertNotNull(creationContext);
                        assertNull(creationContext.getScheduled());
                        assertEquals(
                            creationContext.getCreator(),
                            "actor://testcluster/test/shards/1/greeter"
                        );
                        assertEquals(
                            creationContext.getCreatorType(),
                            shorten(GreetingActor.class)
                        );
                        TraceContext traceContext = scope.getTraceContext();
                        assertNotNull(traceContext);
                        assertNotEquals(traceContext.getParentId(), TEST_TRACE.getSpanId());
                        assertEquals(traceContext.getTraceId(), TEST_TRACE.getTraceId());
                        assertNotEquals(traceContext.getSpanId(), TEST_TRACE.getSpanId());
                    }
                })
                .onReceive(Greeting.class, m -> {
                    logger.info("Got Greeting from {}", m.getWho());
                    if (getManager().isTracingEnabled()) {
                        MessagingScope scope = getManager().currentScope();
                        assertNotNull(scope);
                        assertNull(scope.getMethod());
                        CreationContext creationContext = scope.getCreationContext();
                        assertNotNull(creationContext);
                        assertNotNull(creationContext.getScheduled());
                        assertTrue(creationContext.getScheduled());
                        assertEquals(
                            creationContext.getCreator(),
                            "actor://testcluster/test/shards/1/greeter"
                        );
                        assertEquals(
                            creationContext.getCreatorType(),
                            shorten(GreetingActor.class)
                        );
                        TraceContext traceContext = scope.getTraceContext();
                        assertNotNull(traceContext);
                        assertNotEquals(traceContext.getParentId(), TEST_TRACE.getSpanId());
                        assertEquals(traceContext.getTraceId(), TEST_TRACE.getTraceId());
                        assertNotEquals(traceContext.getSpanId(), TEST_TRACE.getSpanId());
                    }
                })
                .orElse(MessageConsumer.noop())
                .postReceive(countDownLatch::countDown)
                .build());

        greeter.tell(new Greeting("Joost van de Wijgerd"), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void testGreeting_client() throws Exception {

        logger.info("Starting testGreeting_client");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorSystem clientActorSystem = testActorSystem.getRemoteActorSystem();
        ActorRef greeter = actorSystem.actorOf("greeter",GreetingActor.class,new StringState("Hello World"));

        //ScheduledMessageRef messageRef = actorSystem.getScheduler().scheduleOnce(null,new Greeting("Delayed Message"),greeter,2, TimeUnit.SECONDS);

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .deleteAfterReceive(false)
                .onReceive(ScheduledGreeting.class, () -> logger.info("Got Scheduled Greeting"))
                .onReceive(Greeting.class, m -> logger.info("Got Greeting from {}", m.getWho()))
                .orElse(MessageConsumer.noop())
                .postReceive(countDownLatch::countDown)
                .build());

        clientActorSystem.actorFor("greeter")
                .tell(new Greeting("This will error out because we use a local queue"));
        greeter.tell(new Greeting("Joost van de Wijgerd"), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));

        assertEquals(LocalMessageQueue.getThrownExceptions().size(), 1);
        assertTrue(LocalMessageQueue.getThrownExceptions().get(0) instanceof UnsupportedOperationException);

        testActorSystem.destroy();
    }

    @Test
    public void testGreeting_client_createActor() throws Exception {

        logger.info("Starting testGreeting_client_createActor");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem clientActorSystem = testActorSystem.getRemoteActorSystem();
        ActorRef greeter = clientActorSystem.actorOf("greeter",GreetingActor.class,new StringState("Hello World"));

        //ScheduledMessageRef messageRef = actorSystem.getScheduler().scheduleOnce(null,new Greeting("Delayed Message"),greeter,2, TimeUnit.SECONDS);

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clientActorSystem.actorFor("greeter")
                .tell(new Greeting("This will error out because we use a local queue"));
        greeter.tell(new Greeting("Joost van de Wijgerd"));

        assertFalse(countDownLatch.await(5, TimeUnit.SECONDS));

        assertEquals(LocalMessageQueue.getThrownExceptions().size(), 3);
        LocalMessageQueue.getThrownExceptions().forEach(e -> assertTrue(e instanceof UnsupportedOperationException));

        testActorSystem.destroy();
    }

    @Test
    public void testStopAfterGreeting() throws Exception {

        logger.info("Starting testStopAfterGreeting");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef greeter = actorSystem.actorOf("greeter",GreetingActor.class,new StringState("Hello World"));

        //ScheduledMessageRef messageRef = actorSystem.getScheduler().scheduleOnce(null,new Greeting("Delayed Message"),greeter,2, TimeUnit.SECONDS);

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .deleteAfterReceive(false)
                .onReceive(ScheduledGreeting.class, () -> {
                    logger.info("Got Scheduled Greeting");
                    stopActor();
                })
                .onReceive(Greeting.class, m -> logger.info("Got Greeting from {}", m.getWho()))
                .orElse(MessageConsumer.noop())
                .postReceive(countDownLatch::countDown)
                .build());

        greeter.tell(new Greeting("Joost van de Wijgerd"), replyActor);

        assertFalse(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void testGreeting_messageSupertype() throws Exception {

        logger.info("Starting testGreeting_messageSupertype");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef greeter = actorSystem.actorOf("greeter",GreetingActor.class,new StringState("Hello World"));

        //ScheduledMessageRef messageRef = actorSystem.getScheduler().scheduleOnce(null,new Greeting("Delayed Message"),greeter,2, TimeUnit.SECONDS);

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .deleteAfterReceive(false)
                .onReceive(IScheduledGreeting.class, () -> logger.info("Got Scheduled Greeting"))
                .onReceive(Greeting.class, m -> logger.info("Got Greeting from {}", m.getWho()))
                .orElse(MessageConsumer.noop())
                .postReceive(countDownLatch::countDown)
                .build());

        greeter.tell(new Greeting("Joost van de Wijgerd"), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

    @Test
    public void testGreeting_client_messageSupertype() throws Exception {

        logger.info("Starting testGreeting_client_messageSupertype");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorSystem clientActorSystem = testActorSystem.getRemoteActorSystem();
        ActorRef greeter = actorSystem.actorOf("greeter",GreetingActor.class,new StringState("Hello World"));

        //ScheduledMessageRef messageRef = actorSystem.getScheduler().scheduleOnce(null,new Greeting("Delayed Message"),greeter,2, TimeUnit.SECONDS);

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .deleteAfterReceive(false)
                .onReceive(IScheduledGreeting.class, () -> logger.info("Got Scheduled Greeting"))
                .onReceive(Greeting.class, m -> logger.info("Got Greeting from {}", m.getWho()))
                .orElse(MessageConsumer.noop())
                .postReceive(countDownLatch::countDown)
                .build());

        clientActorSystem.actorFor("greeter")
                .tell(new Greeting("This will error out because we use a local queue"));
        greeter.tell(new Greeting("Joost van de Wijgerd"), replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));

        assertEquals(LocalMessageQueue.getThrownExceptions().size(), 1);
        assertTrue(LocalMessageQueue.getThrownExceptions().get(0) instanceof UnsupportedOperationException);

        testActorSystem.destroy();
    }

    @Test
    public void testStopAfterGreeting_messageSupertype() throws Exception {

        logger.info("Starting testStopAfterGreeting_messageSupertype");

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef greeter = actorSystem.actorOf("greeter",GreetingActor.class,new StringState("Hello World"));

        //ScheduledMessageRef messageRef = actorSystem.getScheduler().scheduleOnce(null,new Greeting("Delayed Message"),greeter,2, TimeUnit.SECONDS);

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
                .deleteAfterReceive(false)
                .onReceive(IScheduledGreeting.class, () -> {
                    logger.info("Got Scheduled Greeting");
                    stopActor();
                })
                .onReceive(Greeting.class, m -> logger.info("Got Greeting from {}", m.getWho()))
                .orElse(MessageConsumer.noop())
                .postReceive(countDownLatch::countDown)
                .build());

        greeter.tell(new Greeting("Joost van de Wijgerd"), replyActor);

        assertFalse(countDownLatch.await(5, TimeUnit.SECONDS));
        assertTrue(LocalMessageQueue.getThrownExceptions().isEmpty());

        testActorSystem.destroy();
    }

}
