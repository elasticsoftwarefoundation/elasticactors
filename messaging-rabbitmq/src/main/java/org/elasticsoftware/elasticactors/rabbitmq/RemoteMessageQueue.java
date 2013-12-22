package org.elasticsoftware.elasticactors.rabbitmq;

import com.rabbitmq.client.Channel;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public class RemoteMessageQueue implements MessageQueue {
    private final Logger logger;
    private final Channel producerChannel;
    private final String exchangeName;
    private final String queueName;

    public RemoteMessageQueue(Channel producerChannel, String exchangeName, String queueName) {
        this.producerChannel = producerChannel;
        this.exchangeName = exchangeName;
        this.queueName = queueName;
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
        throw new UnsupportedOperationException("Remote queues cannot be polled");
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public void initialize() throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void destroy() {
        // nothing to do, channel is reused across other remote queues as well
    }
}
