package org.elasticsoftware.elasticactors.rabbitmq;

/**
 * @author Joost van de Wijgerd
 */
public interface MessageAcker {
    void deliver(long deliveryTag);

    void ack(long deliveryTag);

    void start();

    void stop();

    public enum Type {
        DIRECT,BUFFERED,WRITE_BEHIND
    }
}
