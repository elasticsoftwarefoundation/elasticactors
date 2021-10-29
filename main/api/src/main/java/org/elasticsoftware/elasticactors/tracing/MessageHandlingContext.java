package org.elasticsoftware.elasticactors.tracing;

import org.elasticsoftware.elasticactors.ActorContext;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsoftware.elasticactors.tracing.TracingUtils.safeToString;
import static org.elasticsoftware.elasticactors.tracing.TracingUtils.shorten;

public final class MessageHandlingContext {

    private static final ConcurrentMap<String, String> shortenCache = new ConcurrentHashMap<>();

    private final String messageType;
    private final String sender;
    private final String receiver;
    private final String receiverType;

    public MessageHandlingContext(
            @Nullable ActorContext actorContext,
            @Nullable TracedMessage tracedMessage) {
        this.messageType = getMessageType(tracedMessage);
        this.sender = tracedMessage != null ? safeToString(tracedMessage.getSender()) : null;
        this.receiver = actorContext != null ? safeToString(actorContext.getSelf()) : null;
        this.receiverType = actorContext != null ? shorten(actorContext.getSelfType()) : null;
    }

    @Nullable
    private static String getMessageType(@Nullable TracedMessage tracedMessage) {
        if (tracedMessage != null) {
            if (tracedMessage.getType() != null) {
                // Let the cache in TracingUtils cache this
                return shorten(tracedMessage.getType());
            } else {
                String typeAsString = tracedMessage.getTypeAsString();
                // This is used in DefaultInternalMessage a lot
                // Let's cache it here instead of in TracingUtils.
                return shortenCache.computeIfAbsent(typeAsString, TracingUtils::shorten);
            }
        }
        return null;
    }

    @Nullable
    public String getMessageType() {
        return messageType;
    }

    @Nullable
    public String getSender() {
        return sender;
    }

    @Nullable
    public String getReceiver() {
        return receiver;
    }

    @Nullable
    public String getReceiverType() {
        return receiverType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MessageHandlingContext)) {
            return false;
        }
        MessageHandlingContext that = (MessageHandlingContext) o;
        return Objects.equals(messageType, that.messageType) &&
                Objects.equals(sender, that.sender) &&
                Objects.equals(receiver, that.receiver) &&
                Objects.equals(receiverType, that.receiverType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageType, sender, receiver, receiverType);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", MessageHandlingContext.class.getSimpleName() + "{", "}")
                .add("messageType='" + messageType + "'")
                .add("sender='" + sender + "'")
                .add("receiver='" + receiver + "'")
                .add("receiverType='" + receiverType + "'")
                .toString();
    }

}
