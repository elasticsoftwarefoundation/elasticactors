package org.elasticsoftware.elasticactors;

/**
 * @author Joost van de Wijgerd
 */
public class TestMessageHandlers {
    @MessageHandler
    public void handle(AnotherTestMessage message,ActorRef sender,TestActorState state,ActorSystem actorSystem) {
        state.setCallSucceeded(true);
        state.setActorSystem(actorSystem);
        state.setMessage(message);
        state.setSender(sender);
    }
}
