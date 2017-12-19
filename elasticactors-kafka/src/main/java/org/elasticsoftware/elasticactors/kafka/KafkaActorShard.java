package org.elasticsoftware.elasticactors.kafka;

import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.ActorShardRef;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.InternalMessageImpl;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public final class KafkaActorShard implements ActorShard {
    private final ShardKey key;
    private final ActorRef myRef;
    private final AtomicReference<PhysicalNode> owningNode = new AtomicReference<>(null);
    private final KafkaActorThread actorThread;
    private final InternalActorSystem actorSystem;

    public KafkaActorShard(ShardKey key, KafkaActorThread actorThread, InternalActorSystem actorSystem) {
        this.key = key;
        this.myRef = new ActorShardRef(actorSystem, actorSystem.getParent().getClusterName(), this);
        this.actorThread = actorThread;
        this.actorSystem = actorSystem;
        // assign this shard to the thread
        this.actorThread.assign(this);
    }

    @Override
    public ShardKey getKey() {
        return key;
    }

    @Override
    public PhysicalNode getOwningNode() {
        return owningNode.get();
    }

    public void setOwningNode(PhysicalNode owningNode) {
        this.owningNode.set(owningNode);
    }

    @Override
    public ActorRef getActorRef() {
        return myRef;
    }

    @Override
    public void sendMessage(ActorRef sender, ActorRef receiver, Object message) throws Exception {
        sendMessage(sender, ImmutableList.of(receiver), message);
    }

    @Override
    public void sendMessage(ActorRef sender, List<? extends ActorRef> receivers, Object message) throws Exception {
        offerInternalMessage(createInternalMessage(sender, receivers, message));
    }

    @Override
    public void undeliverableMessage(InternalMessage message, ActorRef receiverRef) throws Exception {
        InternalMessage undeliverableMessage = new InternalMessageImpl( receiverRef,
                message.getSender(),
                message.getPayload(),
                message.getPayloadClass(),
                message.isDurable(),
                true,
                message.getTimeout());
        offerInternalMessage(undeliverableMessage);
    }

    @Override
    public void offerInternalMessage(InternalMessage internalMessage) {
        // the calling thread can be a KafkaActorThread (meaning this is called as a side effect of handling another message)
        // or it is called from another thread in which case it is not part of an existing transaction
        actorThread.send(key, internalMessage);
    }

    public void schedule(ScheduledMessage scheduledMessage) {
        // forward this to the ActorThread
        actorThread.schedule(key, scheduledMessage);
    }

    KafkaActorThread getActorThread() {
        return actorThread;
    }

    @Override
    public void init() throws Exception {

    }

    @Override
    public void destroy() {

    }

    private InternalMessage createInternalMessage(ActorRef from, List<? extends ActorRef> to, Object message) throws IOException {
        MessageSerializer<Object> messageSerializer = (MessageSerializer<Object>) actorSystem.getSerializer(message.getClass());
        if(messageSerializer == null) {
            throw new IllegalArgumentException("MessageSerializer not found for message of type "+message.getClass().getName());
        }
        // get the durable flag
        Message messageAnnotation = message.getClass().getAnnotation(Message.class);
        final boolean durable = (messageAnnotation != null) && messageAnnotation.durable();
        final int timeout = (messageAnnotation != null) ? messageAnnotation.timeout() : Message.NO_TIMEOUT;
        return new InternalMessageImpl(from, ImmutableList.copyOf(to), SerializationContext.serialize(messageSerializer, message),message.getClass().getName(),durable, timeout);
    }
}
