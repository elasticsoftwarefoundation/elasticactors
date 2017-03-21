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

package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;

/**
 * @author Joost van de Wijgerd
 */
@Actor(serializationFramework = JacksonSerializationFramework.class)
public class AskForGreetingActor extends MethodActor {
    @MessageHandler
    public void handle(AskForGreeting greeting, ActorSystem actorSystem, ActorRef replyActor) {
        try {
            ActorRef echo = actorSystem.actorOf("echo", EchoGreetingActor.class);
            System.out.println("Got REQUEST in Thread" + Thread.currentThread().getName());

            echo.ask(new Greeting("echo"), Greeting.class).whenComplete((g, throwable) -> {
                System.out.println("Got REPLY in Thread" + Thread.currentThread().getName());
                replyActor.tell(g);
            });
        } catch(Exception e) {
            
        }
    }
}
