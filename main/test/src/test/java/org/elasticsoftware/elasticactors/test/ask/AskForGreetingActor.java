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

package org.elasticsoftware.elasticactors.test.ask;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.MessageHandlers;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.base.state.StringState;
import org.elasticsoftware.elasticactors.runtime.PluggableMessageHandlersRegistry;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;
import org.elasticsoftware.elasticactors.test.common.EchoGreetingActor;
import org.elasticsoftware.elasticactors.test.common.Greeting;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Joost van de Wijgerd
 */
@Actor(serializationFramework = JacksonSerializationFramework.class, stateClass = StringState.class)
@MessageHandlers(registryClass = PluggableMessageHandlersRegistry.class, value = {AfterHandlers.class, BeforeHandlers.class})
@PersistenceConfig(excluded = {AskForGreeting.class})
public class AskForGreetingActor extends MethodActor {

    static final ThreadLocal<Integer> messageHandlerCounter = ThreadLocal.withInitial(() -> 0);

    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
        messageHandlerCounter.remove();
        super.onReceive(sender, message);
    }

    @MessageHandler(order = 0)
    public void handle(AskForGreeting greeting, ActorSystem actorSystem, ActorRef replyActor, StringState state) throws Exception {
        ActorRef echo = actorSystem.actorOf(
            "echo" + (state != null && state.getStringBody() != null ? state.getStringBody() : ""),
            EchoGreetingActor.class
        );
        if (logger.isInfoEnabled()) {
            logger.info("Got REQUEST in Thread {}", Thread.currentThread().getName());
        }
        checkHandlerOrder(1);
        if (getManager().isTracingEnabled()) {
            MessagingScope scope = getManager().currentScope();
            assertNotNull(scope);
            assertEquals(
                scope.getMethod(),
                AskForGreetingActor.class.getMethod(
                    "handle",
                    AskForGreeting.class,
                    ActorSystem.class,
                    ActorRef.class,
                    StringState.class
                )
            );
        }
        echo.ask(new Greeting("echo"), Greeting.class, greeting.getPersistOnResponse())
                .whenComplete((g, e) -> {
                    if (g != null) {
                        if (logger.isInfoEnabled()) {
                            logger.info("Got REPLY in Thread {}", Thread.currentThread().getName());
                        }
                        replyActor.tell(g);
                    } else if (e != null) {
                        logger.error("Got an unexpected exception", e);
                        replyActor.tell(e);
                    }
                });
    }

    static void checkHandlerOrder(int i) {
        int currentHandler = messageHandlerCounter.get();
        assertEquals(i, currentHandler);
        messageHandlerCounter.set(currentHandler + 1);
    }
}
