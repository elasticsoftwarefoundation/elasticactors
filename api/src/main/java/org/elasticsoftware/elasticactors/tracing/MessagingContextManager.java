package org.elasticsoftware.elasticactors.tracing;

import org.elasticsoftware.elasticactors.ActorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.function.Function;

public final class MessagingContextManager {

    private final static Logger logger = LoggerFactory.getLogger(MessagingContextManager.class);

    // Tracing headers
    public static final String SPAN_ID_HEADER = "X-B3-SpanId";
    public static final String TRACE_ID_HEADER = "X-B3-TraceId";
    public static final String PARENT_SPAN_ID_HEADER = "X-B3-ParentSpanId";

    // Receiving-side headers
    public static final String MESSAGE_TYPE_KEY = "messageType";
    public static final String SENDER_KEY = "sender";
    public static final String RECEIVER_KEY = "receiver";
    public static final String RECEIVER_TYPE_KEY = "receiverType";
    public static final String RECEIVER_METHOD_KEY = "receiverMethod";

    // Originating-side headers
    public static final String CREATOR_KEY = "creator";
    public static final String CREATOR_TYPE_KEY = "creatorType";
    public static final String CREATOR_METHOD_KEY = "creatorMethod";
    public static final String SCHEDULED_KEY = "scheduled";


    @Nullable
    public static TraceContext currentTraceContext() {
        TraceContextManager currentManager = TraceContextManager.threadContext.get();
        return currentManager != null ? currentManager.context : null;
    }

    @Nullable
    public static MessageHandlingContext currentMessageHandlingContext() {
        MessageHandlingContextManager currentManager =
                MessageHandlingContextManager.threadContext.get();
        return currentManager != null ? currentManager.context : null;
    }

    @Nullable
    public static CreationContext currentCreationContext() {
        CreationContextManager currentManager = CreationContextManager.threadContext.get();
        return currentManager != null ? currentManager.context : null;
    }

    @Nullable
    public static Method currentMethodContext() {
        MethodContextManager currentManager = MethodContextManager.threadContext.get();
        return currentManager != null ? currentManager.context : null;
    }

    @Nonnull
    public static MessagingScope enter(
            @Nullable ActorContext context,
            @Nullable TracedMessage message) {
        return new MessagingScope(
                MessageHandlingContextManager.enter(new MessageHandlingContext(context, message)),
                TraceContextManager.enter(new TraceContext(message != null
                        ? message.getTraceContext()
                        : null)),
                message != null && message.getCreationContext() != null
                        ? CreationContextManager.enter(message.getCreationContext())
                        : null);
    }

    @Nonnull
    public static MessagingScope enter(
            @Nullable TraceContext traceContext,
            @Nullable CreationContext creationContext) {
        return new MessagingScope(
                traceContext != null ? TraceContextManager.enter(traceContext) : null,
                creationContext != null ? CreationContextManager.enter(creationContext) : null);
    }

    @Nonnull
    public static MessagingScope enter(@Nonnull Method context) {
        return new MessagingScope(MethodContextManager.enter(context));
    }

    private interface ContextManager extends AutoCloseable {

        @Nonnull
        Object getContext();
    }

    public final static class MessagingScope implements AutoCloseable {

        private final ContextManager[] contextManagers;

        public MessagingScope(@Nonnull ContextManager... contextManagers) {
            this.contextManagers = Objects.requireNonNull(contextManagers);
        }

        @Override
        public void close() {
            for (ContextManager cm : contextManagers) {
                if (cm != null) {
                    try {
                        cm.close();
                    } catch (Exception e) {
                        logger.error("Exception thrown while closing {}", cm.getContext(), e);
                    }
                }
            }
        }
    }

    private final static class TraceContextManager implements ContextManager {

        private static final ThreadLocal<TraceContextManager> threadContext = new ThreadLocal<>();

        private final TraceContext context;

        private TraceContextManager(@Nonnull TraceContext context) {
            this.context = Objects.requireNonNull(context);
        }

        @Nonnull
        @Override
        public TraceContext getContext() {
            return context;
        }

        @Nonnull
        private static TraceContextManager enter(@Nonnull TraceContext context) {
            TraceContextManager newManager = new TraceContextManager(context);
            logEnter(threadContext, newManager);
            addToLogContext(SPAN_ID_HEADER, context, TraceContext::getSpanId);
            addToLogContext(TRACE_ID_HEADER, context, TraceContext::getTraceId);
            addToLogContext(PARENT_SPAN_ID_HEADER, context, TraceContext::getParentSpanId);
            threadContext.set(newManager);
            return newManager;
        }

        @Override
        public void close() {
            logClose(threadContext, this);
            removeFromLogContext(SPAN_ID_HEADER);
            removeFromLogContext(TRACE_ID_HEADER);
            removeFromLogContext(PARENT_SPAN_ID_HEADER);
            threadContext.set(null);
        }

    }

    private final static class MessageHandlingContextManager implements ContextManager {

        private static final ThreadLocal<MessageHandlingContextManager> threadContext =
                new ThreadLocal<>();

        private final MessageHandlingContext context;

        private MessageHandlingContextManager(@Nonnull MessageHandlingContext context) {
            this.context = Objects.requireNonNull(context);
        }

        @Nonnull
        @Override
        public MessageHandlingContext getContext() {
            return context;
        }

        @Nonnull
        private static MessageHandlingContextManager enter(
                @Nonnull MessageHandlingContext context) {
            MessageHandlingContextManager newManager = new MessageHandlingContextManager(context);
            logEnter(threadContext, newManager);
            addToLogContext(MESSAGE_TYPE_KEY, context, MessageHandlingContext::getMessageType);
            addToLogContext(SENDER_KEY, context, MessageHandlingContext::getSender);
            addToLogContext(RECEIVER_KEY, context, MessageHandlingContext::getReceiver);
            addToLogContext(RECEIVER_TYPE_KEY, context, MessageHandlingContext::getReceiverType);
            threadContext.set(newManager);
            return newManager;
        }

        @Override
        public void close() {
            logClose(threadContext, this);
            removeFromLogContext(MESSAGE_TYPE_KEY);
            removeFromLogContext(SENDER_KEY);
            removeFromLogContext(RECEIVER_KEY);
            removeFromLogContext(RECEIVER_TYPE_KEY);
            threadContext.set(null);
        }
    }

    private final static class CreationContextManager implements ContextManager {

        private static final ThreadLocal<CreationContextManager> threadContext =
                new ThreadLocal<>();

        private final CreationContext context;

        @Nonnull
        @Override
        public CreationContext getContext() {
            return context;
        }

        private CreationContextManager(@Nonnull CreationContext context) {
            this.context = Objects.requireNonNull(context);
        }

        @Nonnull
        private static CreationContextManager enter(@Nonnull CreationContext context) {
            CreationContextManager newManager = new CreationContextManager(context);
            logEnter(threadContext, newManager);
            addToLogContext(CREATOR_KEY, context, CreationContext::getCreator);
            addToLogContext(CREATOR_TYPE_KEY, context, CreationContext::getCreatorType);
            addToLogContext(CREATOR_METHOD_KEY, context, CreationContext::getCreatorMethod);
            addToLogContext(SCHEDULED_KEY, context, CreationContext::getScheduled);
            threadContext.set(newManager);
            return newManager;
        }

        @Override
        public void close() {
            logClose(threadContext, this);
            removeFromLogContext(CREATOR_KEY);
            removeFromLogContext(CREATOR_TYPE_KEY);
            removeFromLogContext(CREATOR_METHOD_KEY);
            removeFromLogContext(SCHEDULED_KEY);
            threadContext.set(null);
        }

    }

    private final static class MethodContextManager implements ContextManager {

        private static final ThreadLocal<MethodContextManager> threadContext = new ThreadLocal<>();

        private final Method context;

        @Nonnull
        @Override
        public Method getContext() {
            return context;
        }

        private MethodContextManager(@Nonnull Method context) {
            this.context = Objects.requireNonNull(context);
        }

        @Nonnull
        private static MethodContextManager enter(@Nonnull Method context) {
            MethodContextManager newManager = new MethodContextManager(context);
            logEnter(threadContext, newManager);
            addToLogContext(RECEIVER_METHOD_KEY, context, MessagingContextManager::shorten);
            threadContext.set(newManager);
            return newManager;
        }

        @Override
        public void close() {
            logClose(threadContext, this);
            removeFromLogContext(RECEIVER_METHOD_KEY);
            threadContext.set(null);
        }

    }

    @Nullable
    public static String shorten(@Nullable String s) {
        if (s != null) {
            String[] parts = s.split("\\.");
            for (int i = 0; i < parts.length - 1; i++) {
                if (parts[i].length() > 0) {
                    parts[i] = parts[i].substring(0, 1);
                }
            }
            return String.join(".", parts);
        }
        return null;
    }

    @Nullable
    public static String shorten(@Nullable Class<?> c) {
        if (c != null) {
            return shorten(c.getName());
        }
        return null;
    }

    @Nonnull
    public static String shorten(@Nonnull Class<?>[] p) {
        String[] s = new String[p.length];
        for (int i = 0; i < p.length; i++) {
            s[i] = shorten(p[i]);
        }
        return String.join(", ", s);
    }

    @Nullable
    public static String shorten(@Nullable Method m) {
        if (m != null) {
            return shorten(m.getDeclaringClass().getName()) + "." + m.getName() +
                    "(" + shorten(m.getParameterTypes()) + ")";
        }
        return null;
    }

    private static <T extends ContextManager> void logEnter(
            @Nonnull ThreadLocal<T> threadContext,
            @Nonnull T contextManager) {
        logger.debug("Putting {} in scope", contextManager.getContext());
        T current = threadContext.get();
        if (current != null) {
            logger.error(
                    "Putting {} in scope, but {} is already in scope. "
                            + "Context in scope likely incorrect.",
                    contextManager.getContext(),
                    current.getContext());
        }
    }

    private static <T extends ContextManager> void logClose(
            @Nonnull ThreadLocal<T> threadContext,
            @Nonnull T closingScope) {
        logger.debug("Removing {} from scope", closingScope.getContext());
        T current = threadContext.get();
        if (current != closingScope) {
            logger.error(
                    "Removing {} from scope, but context in scope was actually {}. "
                            + "Context in scope likely incorrect.",
                    closingScope.getContext(),
                    current != null ? current.getContext() : null);
        }
    }

    @Nullable
    private static String safeToString(@Nullable Object o) {
        return o != null ? o.toString() : null;
    }

    private static <D, T> void addToLogContext(
            @Nonnull String key,
            @Nonnull D object,
            @Nonnull Function<D, T> getterFunction) {
        String value = safeToString(getterFunction.apply(object));
        if (value != null) {
            MDC.put(key, value);
        }
    }

    private static void removeFromLogContext(String key) {
        MDC.remove(key);
    }

}
