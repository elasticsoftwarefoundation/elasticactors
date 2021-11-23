/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.test.ask;

import com.google.common.collect.ImmutableMap;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorRefGroup;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.base.actors.ActorDelegate;
import org.elasticsoftware.elasticactors.base.actors.ReplyActor;
import org.elasticsoftware.elasticactors.base.state.StringState;
import org.elasticsoftware.elasticactors.test.TestActorSystem;
import org.elasticsoftware.elasticactors.test.common.EchoGreetingActor;
import org.elasticsoftware.elasticactors.test.common.Greeting;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * @author Joost van de Wijgerd
 */
public class AskTest {

    private final static Logger logger = LoggerFactory.getLogger(AskTest.class);

    private final static ThreadLocal<MessagingScope> testScope = new ThreadLocal<>();

    @BeforeMethod
    public void addExternalCreatorData(Method method) {
        testScope.set(getManager().enter(
            new TraceContext(ImmutableMap.of(
                "testOriginalCreator", this.getClass().getSimpleName(),
                "testAdditionalBaggage", "someValue"
            )),
                new CreationContext(
                        this.getClass().getSimpleName(),
                        this.getClass(),
                        method)));
    }

    @AfterMethod
    public void removeExternalCreatorData() {
        if (getManager().isLogContextProcessingEnabled()) {
            assertEquals(MDC.get("testOriginalCreator"), this.getClass().getSimpleName());
            assertEquals(MDC.get("testAdditionalBaggage"), "someValue");
        }
        testScope.get().close();
        assertNull(getManager().currentScope());
        assertNull(MDC.get("testOriginalCreator"));
        assertNull(MDC.get("testAdditionalBaggage"));
        testScope.remove();
    }

    @Test
    public void testAskGreeting() throws Exception {
        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        logger.info("Starting testAskGreeting");

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef echo = actorSystem.actorOf("e", EchoGreetingActor.class);


        Greeting response = echo.ask(new Greeting("echo"), Greeting.class).toCompletableFuture().get();

        assertEquals(response.getWho(), "echo");

        testActorSystem.destroy();
    }

    @Test
    public void testAskGreetingViaActor() throws Exception {
        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        logger.info("Starting testAskGreetingViaActor");

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef echo = actorSystem.actorOf("ask", AskForGreetingActor.class);


        Greeting response = echo.ask(new AskForGreeting(), Greeting.class).toCompletableFuture().get();

        assertEquals(response.getWho(), "echo");

        testActorSystem.destroy();
    }

    @Test(enabled = false)
    public void testAskGreetingViaActor_stressTest() throws Exception {
        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        logger.info("Starting testAskGreetingViaActor");

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef echo = actorSystem.actorOf("ask", AskForGreetingActor.class);

        ActorRef[] actors = new ActorRef[1_000];

        for (int i = 0; i < actors.length; i++) {
            actors[i] = actorSystem.actorOf("ask" + i, AskForGreetingActor.class, new StringState(Integer.toString(i)));
        }

        CompletableFuture<Greeting> response = echo.ask(new AskForGreeting(), Greeting.class)
            .whenComplete((m, e) -> {
                if (m != null) {
                    logger.info(
                        "TEMP ACTOR got REPLY from {} in Thread {}",
                        m.getWho(),
                        Thread.currentThread().getName()
                    );
                } else if (e != null) {
                    logger.info("GOT ERROR", e);
                    System.exit(1);
                }
            }).toCompletableFuture();

        CompletableFuture<?>[] futures = new CompletableFuture<?>[1_000_000];
        CountDownLatch tempLatch = new CountDownLatch(futures.length);
        Random rand = ThreadLocalRandom.current();
        for (int i = 0; i < futures.length; i++) {
            futures[i] =
                actors[rand.nextInt(actors.length)].ask(new AskForGreeting(), Greeting.class)
                    .whenComplete((m, e) -> {
                        if (m != null) {
                            tempLatch.countDown();
                            logger.info(
                                "TEMP ACTOR got REPLY from {} in Thread {}",
                                m.getWho(),
                                Thread.currentThread().getName()
                            );
                        } else if (e != null) {
                            logger.error("GOT ERROR", e);
                            System.exit(1);
                        }
                    })
                    .toCompletableFuture();
        }

        ActorRefGroup group = actorSystem.groupOf(Arrays.asList(actors));
        CountDownLatch groupLatch = new CountDownLatch(actors.length);
        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class, ActorDelegate.builder()
            .deleteAfterReceive(false)
            .onReceive(
                Greeting.class,
                m -> {
                    logger.info(
                        "Got GROUP count {} current actor name '{}'",
                        actors.length - groupLatch.getCount(),
                        m.getWho()
                    );
                    groupLatch.countDown();
                }
            )
            .orElse((s, m) -> {
                logger.error("Received message of type [{}] from [{}]", m.getClass().getName(), s);
                System.exit(1);
            })
            .postReceive(() -> {
                if (groupLatch.getCount() == 0) {
                    ActorDelegate.Builder.stopActor();
                }
            })
            .onUndeliverable(() -> {
                logger.error("Could not deliver message from {}", ActorContextHolder.getSelf());
                System.exit(1);
            })
            .build());

        group.tell(new AskForGreeting(), replyActor);

        assertTrue(tempLatch.await(60, TimeUnit.SECONDS));
        assertTrue(groupLatch.await(60, TimeUnit.SECONDS));

        for (CompletableFuture<?> future : futures) {
            if (!future.isDone()) {
                try {
                    logger.info("Got {} from the future", future.get().getClass().getName());
                } catch (Exception e) {
                    logger.error("Oops!", e);
                }
                System.exit(1);
            }
        }

        assertEquals(response.get().getWho(), "echo");

        testActorSystem.destroy();
    }

    @Test
    public void testAskGreetingViaActorWithPersistOnReponse() throws Exception {
        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        logger.info("Starting testAskGreetingViaActorWithPersistOnReponse");

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef echo = actorSystem.actorOf("ask", AskForGreetingActor.class);


        Greeting response = echo.ask(new AskForGreeting(true), Greeting.class).toCompletableFuture().get();

        assertEquals(response.getWho(), "echo");

        testActorSystem.destroy();
    }


    @Test
    public void testAskGreetingAsync() throws Exception {
        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        logger.info("Starting testAskGreetingAsync");

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef echo = actorSystem.actorOf("e", EchoGreetingActor.class);

        final CountDownLatch waitLatch = new CountDownLatch(1);
        final AtomicReference<String> response = new AtomicReference<>();

        echo.ask(new Greeting("echo"), Greeting.class).whenComplete((greeting, throwable) -> {
            logger.info("Running whenComplete");
            response.set(greeting.getWho());
            waitLatch.countDown();
        });

        waitLatch.await();

        assertEquals(response.get(), "echo");

        testActorSystem.destroy();
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void testUnexpectedResponse() throws Exception {
        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        logger.info("Starting testUnexpectedResponse");

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef echo = actorSystem.actorOf("e", EchoGreetingActor.class);

        Greeting response = echo.ask(new Greeting("Santa Claus"), Greeting.class).toCompletableFuture().get();

        testActorSystem.destroy();
    }
}
