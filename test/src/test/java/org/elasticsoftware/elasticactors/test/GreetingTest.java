/*
 * Copyright 2013 - 2016 The Original Authors
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

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.base.actors.ActorDelegate;
import org.elasticsoftware.elasticactors.base.actors.ReplyActor;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

/**
 * @author Joost van de Wijgerd
 */
public class GreetingTest {
    @Test
    public void testGreeting() throws Exception {

        TestActorSystem testActorSystem = new TestActorSystem();
        testActorSystem.initialize();

        ActorSystem actorSystem = testActorSystem.getActorSystem();
        ActorRef greeter = actorSystem.actorOf("greeter",GreetingActor.class);

        //ScheduledMessageRef messageRef = actorSystem.getScheduler().scheduleOnce(null,new Greeting("Delayed Message"),greeter,2, TimeUnit.SECONDS);

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        ActorRef replyActor = actorSystem.tempActorOf(ReplyActor.class,new ActorDelegate(false) {
            @Override
            public void onReceive(ActorRef sender, Object message) throws Exception {
                if(message instanceof ScheduledGreeting) {
                    System.out.println("Got Scheduled Greeting");
                } else if(message instanceof Greeting) {
                    System.out.println(format("Got Greeting from %s",((Greeting)message).getWho()));
                }
                countDownLatch.countDown();
            }
        });

        greeter.tell(new Greeting("Joost van de Wijgerd"),replyActor);

        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));

        testActorSystem.destroy();
    }
}
