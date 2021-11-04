package org.elasticsoftware.elasticactors.messaging;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.messaging.internal.InternalHashKeyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;

import static org.elasticsoftware.elasticactors.messaging.SplittableUtils.calculateBucketForEmptyOrSingleActor;

public final class MultiMessageQueueProxy implements MessageQueueProxy {

    private final static Logger logger =
        LoggerFactory.getLogger(MultiMessageQueueProxy.class);

    private final Hasher hasher;

    private final MessageQueueFactory messageQueueFactory;
    private final MessageHandler messageHandler;
    private final ActorRef actorRef;
    private final MessageQueue[] messageQueues;

    public MultiMessageQueueProxy(
        Hasher hasher,
        MessageQueueFactory messageQueueFactory,
        MessageHandler messageHandler,
        ActorRef actorRef,
        int queueCount)
    {
        this.hasher = hasher;
        this.messageQueueFactory = messageQueueFactory;
        this.messageHandler = messageHandler;
        this.actorRef = actorRef;
        if (queueCount <= 0) {
            throw new IllegalArgumentException("Number of queues must be greater than 0");
        }
        this.messageQueues = new MessageQueue[queueCount];
    }

    @Override
    public synchronized void init() throws Exception {
        logger.info(
            "Initializing queue proxy for [{}/{}] in Multi-Queue mode with {} queues",
            actorRef.getActorCluster(),
            actorRef.getActorPath(),
            messageQueues.length
        );
        // for backwards compatibility, the first node queue maintains the regular name
        messageQueues[0] = messageQueueFactory.create(actorRef.getActorPath(), messageHandler);
        for (int i = 1; i < messageQueues.length; i++) {
            messageQueues[i] = messageQueueFactory.create(
                actorRef.getActorPath() + "-queue-" + i,
                messageHandler
            );
        }
    }

    @Override
    public void destroy() {
        logger.info(
            "Destroying queue proxy for [{}/{}]",
            actorRef.getActorCluster(),
            actorRef.getActorPath()
        );
        // release all resources
        for (MessageQueue messageQueue : messageQueues) {
            messageQueue.destroy();
        }
    }

    @Override
    public void offerInternalMessage(InternalMessage message) {
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
                    int bucket = calculateBucketForEmptyOrSingleActor(
                        message.getReceivers(),
                        hasher,
                        messageQueues.length
                    );
                    // Compute a queue for this message
                    sendToBucket(bucket, message);
                } else if (message instanceof Splittable) {
                    Map<Integer, InternalMessage> messagesPerBucket =
                        ((Splittable<String, InternalMessage>) message).splitInBuckets(
                            hasher,
                            messageQueues.length
                        );
                    messagesPerBucket.forEach(this::sendToBucket);
                }
            }
        }
    }

    private int getBucket(String messageQueueKey) {
        return Math.abs(hasher.hashStringToInt(messageQueueKey)) % messageQueues.length;
    }

    private void sendToBucket(int bucket, InternalMessage message) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "Offering message of type [{}] wrapped in a [{}] to [{}] on queue {}",
                message.getPayloadClass(),
                message.getClass().getName(),
                message.getReceivers(),
                bucket
            );
        }
        messageQueues[bucket].offer(message);
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
