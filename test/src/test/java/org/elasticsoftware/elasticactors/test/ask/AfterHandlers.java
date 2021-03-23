package org.elasticsoftware.elasticactors.test.ask;

import org.elasticsoftware.elasticactors.MessageHandler;
import org.elasticsoftware.elasticactors.MethodActor;

public class AfterHandlers extends MethodActor {

    @MessageHandler(order = MessageHandler.LOWEST_PRECEDENCE)
    public void handleAfter(AskForGreeting greeting) {
        logger.info("Got REQUEST in Thread {} (last handler)", Thread.currentThread().getName());
        AskForGreetingActor.checkHandlerOrder(2);
    }

}
