package org.elasticsoftware.elasticactors.client;

import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageImpl;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;

import java.util.List;

final class ClientActorShard implements ActorShard {

    private final ShardKey key;
    private final String actorPath;
    private final MessageQueueFactory messageQueueFactory;
    private final SerializationFrameworkCache serializationFrameworkCache;

    private MessageQueue messageQueue;

    ClientActorShard(
            ShardKey key,
            MessageQueueFactory messageQueueFactory,
            SerializationFrameworkCache serializationFrameworkCache) {
        this.key = key;
        this.messageQueueFactory = messageQueueFactory;
        this.serializationFrameworkCache = serializationFrameworkCache;
        this.actorPath = String.format("%s/shards/%d", key.getActorSystemName(), key.getShardId());
    }

    @Override
    public ShardKey getKey() {
        return key;
    }

    @Override
    public PhysicalNode getOwningNode() {
        throw new UnsupportedOperationException("Client actor shards are not owned by any nodes");
    }

    @Override
    public ActorRef getActorRef() {
        throw new UnsupportedOperationException("Client actor shards can't receive messages");
    }

    /**
     * A specialization of {@link ActorShard#sendMessage(ActorRef, ActorRef, Object)} for a
     * ClientActorShard. The sender is always ignored.
     */
    @Override
    public void sendMessage(ActorRef sender, ActorRef receiver, Object message) throws Exception {
        sendMessage(sender, ImmutableList.of(receiver), message);
    }

    /**
     * A specialization of {@link ActorShard#sendMessage(ActorRef, List, Object)} for a
     * ClientActorShard. The sender is always ignored.
     */
    @Override
    public void sendMessage(
            ActorRef sender, List<? extends ActorRef> receiver, Object message) throws Exception {
        MessageSerializer messageSerializer = getSerializer(message.getClass());
        // get the durable flag
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        final boolean durable = (messageAnnotation == null) || messageAnnotation.durable();
        final int timeout =
                (messageAnnotation != null) ? messageAnnotation.timeout() : Message.NO_TIMEOUT;
        messageQueue.offer(new InternalMessageImpl(
                null,
                ImmutableList.copyOf(receiver),
                SerializationContext.serialize(messageSerializer, message),
                message.getClass().getName(),
                durable,
                timeout));
    }

    private <T> MessageSerializer<T> getSerializer(Class<T> messageClass) {
        Message messageAnnotation = messageClass.getAnnotation(Message.class);
        if (messageAnnotation != null) {
            SerializationFramework framework = serializationFrameworkCache
                    .getSerializationFramework(messageAnnotation.serializationFramework());
            return framework.getSerializer(messageClass);
        }
        return null;
    }

    @Override
    public void undeliverableMessage(
            InternalMessage undeliverableMessage, ActorRef receiverRef) throws Exception {
        throw new UnsupportedOperationException("Client actor shards can't receive responses");
    }

    @Override
    public void offerInternalMessage(InternalMessage message) {
        throw new UnsupportedOperationException("Client actor shards can't send internal messages");
    }

    @Override
    public void init() throws Exception {
        this.messageQueue = messageQueueFactory.create(actorPath, null);
    }

    @Override
    public void destroy() {
        this.messageQueue.destroy();
    }
}
