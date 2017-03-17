package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;

@Actor(serializationFramework = JacksonSerializationFramework.class)
public class EchoGreetingActor extends MethodActor {

    @MessageHandler
    public void handleGreeting(ActorRef sender, Greeting message) throws Exception {
        if (message.getWho().equals("Santa Claus")) {
            sender.tell(new SpecialGreeting("Santa Claus"));
        } else {
            sender.tell(message);
        }
    }

}
