package org.elasticsoftware.elasticactors.rabbitmq;

import com.rabbitmq.client.*;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public class LocalMessageQueue extends DefaultConsumer implements MessageQueue {
    private final Logger logger;
    private final Channel consumerChannel;
    private final Channel producerChannel;
    private final String exchangeName;
    private final String queueName;
    private final MessageHandler messageHandler;

    public LocalMessageQueue(Channel consumerChannel, Channel producerChannel, String exchangeName, String queueName, MessageHandler messageHandler) {
        super(consumerChannel);
        this.consumerChannel = consumerChannel;
        this.producerChannel = producerChannel;
        this.exchangeName = exchangeName;
        this.queueName = queueName;
        this.messageHandler = messageHandler;
        this.logger = Logger.getLogger(String.format("Producer[%s->%s]",exchangeName,queueName));
    }

    @Override
    public boolean offer(InternalMessage message) {
        // @todo: use the message properties to set the BasicProperties if necessary
        try {
            producerChannel.basicPublish(exchangeName, queueName,true,false,null,message.toByteArray());
            return true;
        } catch (IOException e) {
            // @todo: what to do with the message?
            logger.error("IOException on publish",e);
            return false;
        }
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
        // @todo: possibly loop until we are the only consumer on the queue
        consumerChannel.basicConsume(queueName,false,this);
    }

    @Override
    public void destroy() {
        try {
            consumerChannel.basicCancel(getConsumerTag());
        } catch (IOException e) {
            logger.error("IOException while cancelling consumer",e);
        }
    }

    @Override
    public void handleCancelOk(String consumerTag) {
        // @todo: use this to block on destroy()
    }

    @Override
    public void handleCancel(String consumerTag) throws IOException {
        // we we're cancelled by an outside force, should not happen. treat as an error
        logger.error(String.format("Unexpectedly cancelled: consumerTag = %s",consumerTag));
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        // @todo: some kind of higher level error handling
    }

    @Override
    public void handleRecoverOk(String consumerTag) {

    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        // get the body data
        InternalMessage message = InternalMessageDeserializer.get().deserialize(body);
        messageHandler.handleMessage(message,new Ack(envelope,message));
    }

    private final class Ack implements MessageHandlerEventListener {
        private final Envelope envelope;
        private final InternalMessage message;

        private Ack(Envelope envelope, InternalMessage message) {
            this.envelope = envelope;
            this.message = message;
        }

        @Override
        public void onError(final InternalMessage message,final Throwable exception) {
            logger.error("Exception while handling message, acking anyway",exception);
            onDone(message);
        }

        @Override
        public void onDone(final InternalMessage message) {
            if(this.message != message) {
                throw new IllegalArgumentException(String.format("Trying to ack wrong message: expected %d, got %d ",
                                                                 this.message.hashCode(),message.hashCode()));
            }
            try {
                consumerChannel.basicAck(envelope.getDeliveryTag(),false);
            } catch (IOException e) {
                logger.error("Exception while acking message",e);
            }
        }
    }
}
