package org.elasticsoftware.elasticactors.tracing;

import org.elasticsoftware.elasticactors.ActorContext;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.StringJoiner;

public final class MessageHandlingContext {

    private final String messageType;
    private final String sender;
    private final String receiver;
    private final String receiverType;

    public MessageHandlingContext(
            @Nullable ActorContext actorContext,
            @Nullable TracedMessage tracedMessage) {
        this.messageType = tracedMessage != null ? tracedMessage.getType() : null;
        this.sender = tracedMessage != null ? safeToString(tracedMessage.getSender()) : null;
        this.receiver = actorContext != null ? safeToString(actorContext.getSelf()) : null;
        this.receiverType = actorContext != null ? actorContext.getSelfType() : null;
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
        return new StringJoiner(", ", MessageHandlingContext.class.getSimpleName() + "[", "]")
                .add("messageType='" + messageType + "'")
                .add("sender='" + sender + "'")
                .add("receiver='" + receiver + "'")
                .add("receiverType='" + receiverType + "'")
                .toString();
    }

    private static String safeToString(@Nullable Object o) {
        return o != null ? o.toString() : null;
    }
}
