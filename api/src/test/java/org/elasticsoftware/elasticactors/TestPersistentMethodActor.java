package org.elasticsoftware.elasticactors;

/**
 * @author Joost van de Wijgerd
 */
@Actor(stateClass = TestActorState.class,serializationFramework = TestSerializationFramework.class)
public class TestPersistentMethodActor extends MethodActor {

    public TestPersistentMethodActor() {
        super();
    }

    @MessageHandler
    public void handle(TestMessage message,ActorRef sender,TestActorState state,ActorSystem actorSystem) {
        System.out.println("handle called");
        state.setCallSucceeded(true);
        state.setActorSystem(actorSystem);
        state.setMessage(message);
        state.setSender(sender);
    }
}
