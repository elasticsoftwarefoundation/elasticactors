package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.TransientInternalMessage;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.messaging.internal.DestroyActorMessage;

public abstract class MultiQueueAbstractActorContainer extends AbstractActorContainer {

    private final MessageQueue[] messageQueues;
    private final boolean messageQueueHashingEnabled;

    public MultiQueueAbstractActorContainer(
        MessageQueueFactory messageQueueFactory,
        ActorRef myRef,
        PhysicalNode node,
        int queueCount,
        boolean messageQueueHashingEnabled)
    {
        super(myRef, messageQueueFactory, node);
        if (queueCount <= 0) {
            throw new IllegalArgumentException("Number of queues must be greater than 0");
        }
        this.messageQueues = new MessageQueue[queueCount];
        this.messageQueueHashingEnabled = messageQueueHashingEnabled;
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
            "Starting up [{}] on node [{}] with {} queues. Message queue hashing is {}.",
            myRef.getActorPath(),
            localNode,
            messageQueues.length,
            messageQueueHashingEnabled ? "ENABLED" : "DISABLED"
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
        String key;
        if (messageQueues.length > 1 || messageQueueHashingEnabled) {
            key = determineKey(message);
        } else {
            key = null;
        }
        if (key == null) {
            messageQueues[0].offer(message);
        } else {
            MessageQueue messageQueue;
            if (messageQueues.length == 1) {
                messageQueue = messageQueues[0];
            } else {
                messageQueue = messageQueues[getBucket(key)];
            }
            if (messageQueueHashingEnabled) {
                messageQueue.offer(key, message);
            } else {
                messageQueue.offer(message);
            }
        }
    }

    private int getBucket(String key) {
        return Math.abs(key.hashCode()) % messageQueues.length;
    }

    private String determineKey(InternalMessage message) {
        if (message instanceof TransientInternalMessage) {
            TransientInternalMessage transientInternalMessage =
                (TransientInternalMessage) message;
            Object payload = transientInternalMessage.getPayload(null);
            if (payload instanceof CreateActorMessage) {
                return ((CreateActorMessage) payload).getActorId();
            } else if (payload instanceof DestroyActorMessage) {
                return ((DestroyActorMessage) payload).getActorRef().getActorId();
            }
        }
        if (!message.getReceivers().isEmpty()) {
            return message.getReceivers().get(0).getActorId();
        }
        return null;
    }
}
