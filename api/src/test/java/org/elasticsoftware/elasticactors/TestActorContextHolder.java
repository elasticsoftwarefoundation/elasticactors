package org.elasticsoftware.elasticactors;

/**
 * @author Joost van de Wijgerd
 */
public class TestActorContextHolder extends ActorContextHolder {
    public static ActorContext setContext(ActorContext context) {
        final ActorContext currentContext = threadContext.get();
        threadContext.set(context);
        return currentContext;
    }

    public static ActorContext getAndClearContext() {
        ActorContext state = threadContext.get();
        threadContext.set(null);
        return state;
    }
}
