package org.elasticsoftware.elasticactors.test.ask;

import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.MethodActor;

public class BeforeHandlers extends MethodActor {

    @MessageHandler(order = MessageHandler.HIGHEST_PRECEDENCE)
    public void handleBefore(AskForGreeting greeting) {
        logger.info("Got REQUEST in Thread {} (first handler)", Thread.currentThread().getName());
        AskForGreetingActor.checkHandlerOrder(0);
    }

}
