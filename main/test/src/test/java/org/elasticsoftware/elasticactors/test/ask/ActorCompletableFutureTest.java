/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.test.ask;

import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.concurrent.ActorCompletableFuture;
import org.elasticsoftware.elasticactors.test.TestActorSystem;
import org.elasticsoftware.elasticactors.test.common.EchoGreetingActor;
import org.elasticsoftware.elasticactors.test.common.Greeting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class ActorCompletableFutureTest {

    private final static Logger logger = LoggerFactory.getLogger(ActorCompletableFutureTest.class);

    @Test(expectedExceptions = IllegalStateException.class)
    public void testAskGreeting_throwExceptionIfGetInActorContext() throws Exception {
        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        logger.info("Starting testAskGreeting");

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef echo = actorSystem.actorOf("e", EchoGreetingActor.class);

        // Explanation: the thread where the CompletableFuture is going to be executed is
        // determined by its completion state at the time thenApply is called.
        // If the thenApply has already been called, the thread that completes the future will
        // also execute the function passed to thenApply.
        // If the future is completed first and then thenApply is called, the thread that is
        // calling thenApply will execute the function.
        //
        // This means that, if ActorRef.ask completes before this thread calls thenApply,
        // this thread will run the function, and thus we wouldn't be inside actor context anymore.
        // If ask is only completed after thenApply is called, then the Actor Thread responsible
        // for this temporary actor will run the function, and we'd still be inside actor context.

        try {
            // Asking the reply to be delayed in 1 second so thenApply surely gets called before
            // ask has the chance to complete
            echo.ask(new Greeting("echo", false, 1000L), Greeting.class)
                .thenApply(greeting -> {
                    try {
                        assertTrue(ActorContextHolder.hasActorContext());
                        // Running get inside an actor context
                        // 'get' will throw an IllegalStateException
                        String someString = echo.ask(
                                new Greeting(greeting.getWho() + " but this will throw an "
                                    + "exception"),
                                Greeting.class
                            ).thenApply(g -> {
                                logger.info(
                                    "Got the response and applied an intermediate stage, "
                                        + "but will throw an exception next: {}",
                                    g.getWho()
                                );
                                return g.getWho();
                            })
                            .get();
                        fail("Calling ActorCompletableFuture.get() was expected to throw an exception here");
                        return someString;
                    } catch (IllegalStateException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).get();
            fail("Calling ActorCompletableFuture.get() was expected to throw an exception here");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalStateException) {
                logger.info("Got IllegalStateException, which we are expecting here", e.getCause());
                throw (IllegalStateException) e.getCause();
            } else {
                logger.error("Got an unexpected exception", e.getCause());
                fail("Not expecting an exception here", e.getCause());
            }
        } catch (IllegalStateException e) {
            logger.error("Got an IllegalStateException, but not the one we're expecting", e);
            fail("Not expecting an IllegalStateException here", e);
        } catch (Exception e) {
            logger.error("Got an unexpected exception", e);
            fail("Not expecting an exception here", e);
        } finally {
            testActorSystem.destroy();
        }
    }

    @Test
    public void testAskGreeting_getOutsideOfActorShouldNotThrowException() throws Exception {
        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        logger.info("Starting testAskGreeting");

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef echo = actorSystem.actorOf("e", EchoGreetingActor.class);

        try {
            ActorCompletableFuture<Greeting> future = echo.ask(new Greeting("echo"), Greeting.class);

            // Explanation: the thread where the CompletableFuture is going to be executed is
            // determined by its completion state at the time thenApply is called.
            // If the thenApply has already been called, the thread that completes the future will
            // also execute the function passed to thenApply.
            // If the future is completed first and then thenApply is called, the thread that is
            // calling thenApply will execute the function.
            //
            // This means that, if ActorRef.ask completes before this thread calls thenApply,
            // this thread will run the function, and thus we wouldn't be inside actor context anymore.
            // If ask is only completed after thenApply is called, then the Actor Thread responsible
            // for this temporary actor will run the function, and we'd still be inside actor context.

            // Wait for the original future to complete
            assertEquals(future.get().getWho(), "echo");

            String who = future
                .thenApply(greeting -> {
                    try {
                        assertFalse(ActorContextHolder.hasActorContext());
                        // Running outside actor context
                        // 'get' will NOT throw an IllegalStateException
                        String someString = echo.ask(
                                new Greeting("echo and thenApply"),
                                Greeting.class
                            ).thenApply(g -> {
                                logger.info(
                                    "Got the response and applied an intermediate stage: {}",
                                    g.getWho()
                                );
                                return g.getWho();
                            })
                            .get();
                        assertEquals(someString, "echo and thenApply");
                        return someString;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).get();
            assertEquals(who, "echo and thenApply");
        } catch (Exception e) {
            logger.error("Got an unexpected exception", e);
            fail("Not expecting an exception here", e);
        } finally {
            testActorSystem.destroy();
        }
    }

}
