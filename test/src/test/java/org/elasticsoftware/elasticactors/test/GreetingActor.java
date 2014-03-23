package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.TypedActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.base.state.StringState;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;

import java.util.concurrent.TimeUnit;

/**
 * @author Joost van de Wijgerd
 */
@Actor(stateClass = StringState.class,serializationFramework = JacksonSerializationFramework.class)
public class GreetingActor extends TypedActor<Greeting> {
    @Override
    public void onReceive(ActorRef sender, Greeting message) throws Exception {
        System.out.println("Hello " + message.getWho());
        ScheduledMessageRef messageRef = getSystem().getScheduler().scheduleOnce(getSelf(),new Greeting("Greeting Actor"),sender,1, TimeUnit.SECONDS);
        sender.tell(new ScheduledGreeting(messageRef));
    }
}
