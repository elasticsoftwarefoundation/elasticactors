package org.elasticsoftware.elasticactors.cluster;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorNode;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.NodeKey;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cache.EvictionListener;
import org.elasticsoftware.elasticactors.cache.NodeActorCacheManager;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageImpl;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.TransientInternalMessage;
import org.elasticsoftware.elasticactors.messaging.internal.ActorType;
import org.elasticsoftware.elasticactors.messaging.internal.CreateActorMessage;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;
import org.elasticsoftware.elasticactors.state.PersistentActor;

import java.util.List;

public abstract class AbstractActorNode extends AbstractActorContainer implements ActorNode, EvictionListener<PersistentActor<NodeKey>> {
    private final NodeKey nodeKey;
    protected final MessageSerializationRegistry messageSerializationRegistry;

    AbstractActorNode(MessageQueueFactory messageQueueFactory,
                      ActorRef myRef,
                      PhysicalNode node,
                      String actorSystemName,
                      MessageSerializationRegistry messageSerializationRegistry) {
        super(messageQueueFactory, myRef, node);
        this.nodeKey = new NodeKey(actorSystemName, node.getId());
        this.messageSerializationRegistry = messageSerializationRegistry;
    }

    @Override
    public void onEvicted(PersistentActor<NodeKey> value) {
        // @todo: a temporary actor that gets evicted is actually being destroyed
    }

    @Override
    public NodeKey getKey() {
        return nodeKey;
    }

    public void sendMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws Exception {
        // we need some special handling for the CreateActorMessage in case of Temp Actor
        if(CreateActorMessage.class.equals(message.getClass()) && ActorType.TEMP.equals(CreateActorMessage.class.cast(message).getType())) {
            messageQueue.offer(new TransientInternalMessage(from, ImmutableList.copyOf(to),message));
        } else {
            // get the durable flag
            Message messageAnnotation = message.getClass().getAnnotation(Message.class);
            final boolean durable = (messageAnnotation != null) && messageAnnotation.durable();
            final boolean immutable = (messageAnnotation != null) && messageAnnotation.immutable();
            final int timeout = (messageAnnotation != null) ? messageAnnotation.timeout() : Message.NO_TIMEOUT;
            if(durable) {
                // durable so it will go over the bus and needs to be serialized
                MessageSerializer messageSerializer = messageSerializationRegistry.getSerializer(message.getClass());
                messageQueue.offer(new InternalMessageImpl(from, ImmutableList.copyOf(to), SerializationContext.serialize(messageSerializer, message), message.getClass().getName(), true, timeout));
            } else if(!immutable) {
                // it's not durable, but it's mutable so we need to serialize here
                MessageSerializer messageSerializer = messageSerializationRegistry.getSerializer(message.getClass());
                messageQueue.offer(new InternalMessageImpl(from, ImmutableList.copyOf(to), SerializationContext.serialize(messageSerializer, message), message.getClass().getName(), false, timeout));
            } else {
                // as the message is immutable we can safely send it as a TransientInternalMessage
                messageQueue.offer(new TransientInternalMessage(from,ImmutableList.copyOf(to),message));
            }
        }
    }

    @Override
    public void undeliverableMessage(InternalMessage message, ActorRef receiverRef) throws Exception {
        InternalMessage undeliverableMessage;
        if (message instanceof TransientInternalMessage) {
            undeliverableMessage = new TransientInternalMessage(receiverRef, message.getSender(), message.getPayload(null), true);
        } else {
            undeliverableMessage = new InternalMessageImpl(receiverRef,
                    message.getSender(),
                    message.getPayload(),
                    message.getPayloadClass(),
                    message.isDurable(),
                    true,
                    message.getTimeout());
        }
        messageQueue.offer(undeliverableMessage);
    }

    @Override
    public boolean isLocal() {
        return true;
    }
}
