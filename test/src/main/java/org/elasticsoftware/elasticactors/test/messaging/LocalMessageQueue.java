package org.elasticsoftware.elasticactors.test.messaging;

import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;

/**
 * @author Joost van de Wijgerd
 */
public final class LocalMessageQueue implements MessageQueue {
    private final Logger logger;
    private final String queueName;
    private final MessageHandler messageHandler;
    private final TransientAck transientAck = new TransientAck();
    private final ThreadBoundExecutor<String> queueExecutor;

    public LocalMessageQueue(ThreadBoundExecutor<String> queueExecutor,String queueName, MessageHandler messageHandler) {
        this.queueExecutor = queueExecutor;
        this.queueName = queueName;
        this.messageHandler = messageHandler;
        this.logger = Logger.getLogger(String.format("Producer[%s]",queueName));
    }

    @Override
    public boolean offer(final InternalMessage message) {
        // execute on a seperate (thread bound) executor
        queueExecutor.execute(new InternalMessageHandler(queueName,message,messageHandler,transientAck,logger));
        return true;

    }

    @Override
    public boolean add(InternalMessage message) {
        return offer(message);
    }

    @Override
    public InternalMessage poll() {
        return null;
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public void initialize() throws Exception {
    }

    @Override
    public void destroy() {

    }


    private static final class InternalMessageHandler implements ThreadBoundRunnable<String> {
        private final String queueName;
        private final InternalMessage message;
        private final MessageHandler messageHandler;
        private final MessageHandlerEventListener listener;
        private final Logger logger;

        private InternalMessageHandler(String queueName, InternalMessage message, MessageHandler messageHandler, MessageHandlerEventListener listener, Logger logger) {
            this.queueName = queueName;
            this.message = message;
            this.messageHandler = messageHandler;
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public String getKey() {
            return queueName;
        }

        @Override
        public void run() {
            try {
                messageHandler.handleMessage(message,listener);
            } catch(Exception e) {
                logger.error("Unexpected exception on #handleMessage",e);
            }
        }
    }

    private final class TransientAck implements MessageHandlerEventListener {

        @Override
        public void onError(InternalMessage message, Throwable exception) {
            logger.error(String.format("Error handling transient message, payloadClass [%s]",message.getPayloadClass()),exception);
        }

        @Override
        public void onDone(InternalMessage message) {
            // do nothing
        }
    }
}
