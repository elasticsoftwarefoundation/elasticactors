package org.elasticsoftware.elasticactors.tracing;

import org.elasticsoftware.elasticactors.ActorContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Method;

public interface MessagingContextManager {

    // Tracing headers
    String SPAN_ID_HEADER = "X-B3-SpanId";
    String TRACE_ID_HEADER = "X-B3-TraceId";
    String PARENT_SPAN_ID_HEADER = "X-B3-ParentSpanId";

    // Receiving-side headers
    String MESSAGE_TYPE_KEY = "messageType";
    String SENDER_KEY = "sender";
    String RECEIVER_KEY = "receiver";
    String RECEIVER_TYPE_KEY = "receiverType";
    String RECEIVER_METHOD_KEY = "receiverMethod";

    // Originating-side headers
    String CREATOR_KEY = "creator";
    String CREATOR_TYPE_KEY = "creatorType";
    String CREATOR_METHOD_KEY = "creatorMethod";
    String SCHEDULED_KEY = "scheduled";

    interface MessagingScope extends AutoCloseable {

        @Nullable
        TraceContext getTraceContext();

        boolean isClosed();

        @Override
        void close();
    }

    @Nullable
    TraceContext currentTraceContext();

    @Nullable
    MessageHandlingContext currentMessageHandlingContext();

    @Nullable
    CreationContext currentCreationContext();

    @Nullable
    CreationContext creationContextFromScope();

    @Nullable
    Method currentMethodContext();

    @Nonnull
    MessagingScope enter(
            @Nullable ActorContext context,
            @Nullable TracedMessage message);

    @Nonnull
    MessagingScope enter(
            @Nullable TraceContext traceContext,
            @Nullable CreationContext creationContext);

    @Nonnull
    MessagingScope enter(@Nonnull Method context);

}
