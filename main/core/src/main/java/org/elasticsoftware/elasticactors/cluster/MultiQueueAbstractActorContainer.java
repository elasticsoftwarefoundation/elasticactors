package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.Splittable;
import org.elasticsoftware.elasticactors.messaging.internal.InternalHashKeyUtils;

import javax.annotation.Nullable;
import java.util.Map;

import static org.elasticsoftware.elasticactors.messaging.SplittableUtils.calculateHash;

public abstract class MultiQueueAbstractActorContainer extends AbstractActorContainer {

    private final MessageQueue[] messageQueues;

    public MultiQueueAbstractActorContainer(
        MessageQueueFactory messageQueueFactory,
        ActorRef myRef,
        PhysicalNode node,
        int queueCount)
    {
        super(myRef, messageQueueFactory, node);
        if (queueCount <= 0) {
            throw new IllegalArgumentException("Number of queues must be greater than 0");
        }
        this.messageQueues = new MessageQueue[queueCount];
    }

    @Override
    public void init() throws Exception {
        // for backwards compatibility, the first node queue maintains the regular name
        messageQueues[0] = messageQueueFactory.create(myRef.getActorPath(), this);
        for (int i = 1; i < messageQueues.length; i++) {
            messageQueues[i] =
                messageQueueFactory.create(myRef.getActorPath() + "-queue-" + i, this);
        }
        logger.info(
            "Starting up container [{}] on node [{}] with {} queues",
            myRef.getActorPath(),
            localNode,
            messageQueues.length
        );
    }

    @Override
    public void destroy() {
        // release all resources
        for (MessageQueue messageQueue : messageQueues) {
            messageQueue.destroy();
        }
    }

    @Override
    public final void offerInternalMessage(InternalMessage message) {
        if (messageQueues.length == 1) {
            sendToBucket(0, message);
        } else {
            String messageQueueKey = determineMessageQueueKey(message);
            if (messageQueueKey != null) {
                // Compute a queue for this message
                sendToBucket(getBucket(messageQueueKey), message);
            } else {
                if (message.getReceivers() != null && message.getReceivers().size() <= 1) {
                    // Optimizing for the most common case in which we only have one receiver
                    int bucket = calculateHash(message.getReceivers(), this::getBucket);
                    // Compute a queue for this message
                    sendToBucket(bucket, message);
                } else if (message instanceof Splittable) {
                    Map<Integer, InternalMessage> messagesPerBucket =
                        ((Splittable<String, InternalMessage>) message).splitFor(this::getBucket);
                    messagesPerBucket.forEach(this::sendToBucket);
                }
            }
        }
    }

    private void sendToBucket(int bucket, InternalMessage message) {
        messageQueues[bucket].offer(message);
    }

    private int getBucket(String key) {
        return Math.abs(key.hashCode()) % messageQueues.length;
    }

    private String determineMessageQueueKey(InternalMessage message) {
        if (message.getReceivers() != null && message.getReceivers().size() > 1) {
            String payload = getMessageQueueKey(message);
            if (payload != null) {
                logger.error(
                    "Received a message of type [{}] that should be hashed to a specific queue, "
                        + "wrapped in a [{}] but has multiple receivers",
                    message.getPayloadClass(),
                    message.getClass().getName()
                );
            }
            return null;
        } else {
            return getMessageQueueKey(message);
        }
    }

    @Nullable
    private String getMessageQueueKey(InternalMessage message) {
        if (message.hasPayloadObject()) {
            try {
                return InternalHashKeyUtils.getMessageQueueAffinityKey(message.getPayload(null));
            } catch (Exception e) {
                logger.error(
                    "Could not determine hashing key for message of type [{}] wrapped in [{}]",
                    message.getPayloadClass(),
                    message.getClass().getName(),
                    e
                );
            }
        }
        return null;
    }
}
