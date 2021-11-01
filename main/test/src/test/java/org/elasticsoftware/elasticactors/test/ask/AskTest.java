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

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.base.state.StringState;
import org.elasticsoftware.elasticactors.test.TestActorSystem;
import org.elasticsoftware.elasticactors.test.common.EchoGreetingActor;
import org.elasticsoftware.elasticactors.test.common.Greeting;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Joost van de Wijgerd
 */
public class AskTest {

    private final static Logger logger = LoggerFactory.getLogger(AskTest.class);

    private final static ThreadLocal<MessagingScope> testScope = new ThreadLocal<>();

    @BeforeMethod
    public void addExternalCreatorData(Method method) {
        testScope.set(getManager().enter(
                new TraceContext(),
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

        CompletableFuture<Greeting> response = echo.ask(new AskForGreeting(), Greeting.class).toCompletableFuture();

        CompletableFuture<?>[] futures = new CompletableFuture<?>[1_000_000];
        Random rand = ThreadLocalRandom.current();
        for (int i = 0; i < futures.length; i++) {
            futures[i] =
                actors[rand.nextInt(actors.length)].ask(new AskForGreeting(), Greeting.class)
                    .thenAccept(m -> logger.info(
                        "TEMP ACTOR got REPLY from {} in Thread {}",
                        m.getWho(),
                        Thread.currentThread().getName()
                    ))
                    .toCompletableFuture();
        }

        CompletableFuture.allOf(futures).get();

        assertEquals(response.get().getWho(), "echo");

        // If we don't wait, DestroyActor messages will still be in the queue when it gets destroyed
        // Nothing bad actually happens because of this, but it spams the logs and ruins profiling
        Thread.sleep(10_000);

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
