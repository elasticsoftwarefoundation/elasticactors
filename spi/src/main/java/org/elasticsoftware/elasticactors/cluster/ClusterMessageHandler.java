package org.elasticsoftware.elasticactors.cluster;

/**
 * @author Joost van de Wijgerd
 */
public interface ClusterMessageHandler {
    /**
     * Handle a message from the Clustering subsystem. These are messages that can be send by the members to each other.
     *
     * @param message           the message bytes
     * @param senderToken       the id (token) of the sending member
     */
    public void handleMessage(byte[] message,String senderToken);
}
