package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.testng.annotations.Test;

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
        greeter.tell(new Greeting("Joost van de Wijgerd"),null);

        testActorSystem.destroy();
    }
}
