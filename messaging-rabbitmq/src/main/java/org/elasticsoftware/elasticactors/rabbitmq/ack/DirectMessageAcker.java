package org.elasticsoftware.elasticactors.rabbitmq.ack;

import com.rabbitmq.client.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.rabbitmq.MessageAcker;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public final class DirectMessageAcker implements MessageAcker {
    private static final Logger logger = LogManager.getLogger(DirectMessageAcker.class);
    private final Channel consumerChannel;

    public DirectMessageAcker(Channel consumerChannel) {
        this.consumerChannel = consumerChannel;
    }

    @Override
    public void deliver(long deliveryTag) {
        // do nothing as we will directly ack
    }

    @Override
    public void ack(long deliveryTag) {
        try {
            consumerChannel.basicAck(deliveryTag,false);
        } catch (IOException e) {
            logger.error("Exception while acking message", e);
        }
    }

    @Override
    public void start() {
        // nothing to do here
    }

    @Override
    public void stop() {
        // nothing to do here
    }
}