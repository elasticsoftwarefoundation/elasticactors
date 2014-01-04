package org.elasticsoftware.elasticactors;

/**
 * @author Joost van de Wijgerd
 */
public class TestActorState implements ActorState<String,TestActorState> {
    private boolean callSucceeded = false;
    private transient ActorSystem actorSystem = null;
    private ActorRef sender;
    private TestMessage message;

    public TestMessage getMessage() {
        return message;
    }

    public void setMessage(TestMessage message) {
        this.message = message;
    }

    public ActorRef getSender() {
        return sender;
    }

    public void setSender(ActorRef sender) {
        this.sender = sender;
    }

    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    public void setActorSystem(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }



    public boolean isCallSucceeded() {
        return callSucceeded;
    }

    public void setCallSucceeded(boolean callSucceeded) {
        this.callSucceeded = callSucceeded;
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public TestActorState getBody() {
        return this;
    }
}
