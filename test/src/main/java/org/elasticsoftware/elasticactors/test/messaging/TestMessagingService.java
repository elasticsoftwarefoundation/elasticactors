package org.elasticsoftware.elasticactors.test.messaging;

import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MessagingService;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class TestMessagingService implements MessagingService, MessageQueueFactory {
    private final ThreadBoundExecutor<String> queueExecutor;

    public TestMessagingService(ThreadBoundExecutor<String> queueExecutor) {
        this.queueExecutor = queueExecutor;
    }

    @Override
    public void sendWireMessage(String queueName, byte[] serializedMessage, PhysicalNode receiver) throws IOException {
        // not used, there is no remote connection
    }

    @Override
    public MessageQueue create(String name, MessageHandler messageHandler) throws Exception {
        return new LocalMessageQueue(queueExecutor,name,messageHandler);
    }
}
