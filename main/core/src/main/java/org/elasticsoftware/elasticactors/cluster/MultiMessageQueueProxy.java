package org.elasticsoftware.elasticactors.cluster;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.Splittable;
import org.elasticsoftware.elasticactors.messaging.internal.InternalHashKeyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.elasticsoftware.elasticactors.messaging.SplittableUtils.calculateBucketForEmptyOrSingleActor;

public final class MultiMessageQueueProxy implements MessageQueueProxy {

    private final static Logger logger =
        LoggerFactory.getLogger(MultiMessageQueueProxy.class);

    /*
    Using MurmurHash here too for stability across nodes.
    Hashing strings should be enough for that, but we need to make sure we're consistent, so let's
    use the same algorithm we use for selecting a shard for a given actor.

    Using a prime seed here. Without this, murmur3_32 has a heavy bias towards even numbers.
     */
    private final HashFunction hashFunction = Hashing.murmur3_32(53);

    private final MessageQueueFactory messageQueueFactory;
    private final MessageHandler messageHandler;
    private final ActorRef actorRef;
    private final MessageQueue[] messageQueues;

    public MultiMessageQueueProxy(
        MessageQueueFactory messageQueueFactory,
        MessageHandler messageHandler,
        ActorRef actorRef,
        int queueCount)
    {
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
        // for backwards compatibility, the first node queue maintains the regular name
        messageQueues[0] = messageQueueFactory.create(actorRef.getActorPath(), messageHandler);
        for (int i = 1; i < messageQueues.length; i++) {
            messageQueues[i] = messageQueueFactory.create(
                actorRef.getActorPath() + "-queue-" + i,
                messageHandler
            );
        }
        logger.info(
            "Starting up queue proxy for [{}] in Multi-Queue mode with {} queues",
            actorRef.getActorPath(),
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
                        this::hashString,
                        messageQueues.length
                    );
                    // Compute a queue for this message
                    sendToBucket(bucket, message);
                } else if (message instanceof Splittable) {
                    Map<Integer, InternalMessage> messagesPerBucket =
                        ((Splittable<String, InternalMessage>) message).splitInBuckets(
                            this::hashString,
                            messageQueues.length
                        );
                    messagesPerBucket.forEach(this::sendToBucket);
                }
            }
        }
    }

    private int getBucket(String messageQueueKey) {
        return Math.abs(hashString(messageQueueKey)) % messageQueues.length;
    }

    private int hashString(String key) {
        int hash = hashFunction.hashString(key, StandardCharsets.UTF_8).asInt();
        logger.debug("Hashed value {} for key [{}]", hash, key);
        return hash;
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
