package org.elasticsoftware.elasticactors.test;

import org.apache.logging.log4j.LogManager;
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
