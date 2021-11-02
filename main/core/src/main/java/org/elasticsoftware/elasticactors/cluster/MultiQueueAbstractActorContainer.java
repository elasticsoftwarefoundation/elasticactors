package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.Splittable;
import org.elasticsoftware.elasticactors.messaging.internal.Hashable;

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
            messageQueues[0].offer(message);
        } else {
            String key = determineKey(message);
            if (key != null) {
                // Compute a queue for this message
                messageQueues[getBucket(key)].offer(message);
            } else {
                if (message.getReceivers() != null && message.getReceivers().size() <= 1) {
                    // Optimizing for the most common case in which we only have one receiver
                    int bucket = calculateHash(message.getReceivers(), this::getBucket);
                    // Compute a queue for this message
                    messageQueues[bucket].offer(message);
                } else if (message instanceof Splittable) {
                    Map<Integer, InternalMessage> messagesPerBucket =
                        ((Splittable<String, InternalMessage>) message).splitFor(this::getBucket);
                    for (Map.Entry<Integer, InternalMessage> entry : messagesPerBucket.entrySet()) {
                        Integer bucket = entry.getKey();
                        InternalMessage splittedMessage = entry.getValue();
                        // Compute a queue for each split message
                        messageQueues[bucket].offer(splittedMessage);
                    }
                }
            }
        }
    }

    private int getBucket(String key) {
        return Math.abs(key.hashCode()) % messageQueues.length;
    }

    private String determineKey(InternalMessage message) {
        if (message.getReceivers() != null && message.getReceivers().size() > 1) {
            Hashable<String> payload = getPayload(message);
            if (payload != null) {
                logger.error(
                    "Received a message of type [{}] that implements [{}] wrapped in a [{}] but "
                        + "has multiple receivers",
                    message.getPayloadClass(),
                    Hashable.class.getName(),
                    message.getClass().getName()
                );
            }
            return null;
        } else {
            Hashable<String> payload = getPayload(message);
            return payload != null ? payload.getHashKey() : null;
        }
    }

    @Nullable
    private Hashable<String> getPayload(InternalMessage message) {
        if (message.hasPayloadObject()) {
            try {
                return Hashable.getIfHashable(message.getPayload(null));
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
