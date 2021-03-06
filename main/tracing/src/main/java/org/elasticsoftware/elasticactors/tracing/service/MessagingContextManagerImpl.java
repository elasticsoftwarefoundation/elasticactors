package org.elasticsoftware.elasticactors.tracing.service;

import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessageHandlingContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager;
import org.elasticsoftware.elasticactors.tracing.NoopMessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.elasticsoftware.elasticactors.tracing.TracedMessage;
import org.elasticsoftware.elasticactors.tracing.TracingUtils;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsoftware.elasticactors.tracing.TracingUtils.safeToString;
import static org.elasticsoftware.elasticactors.tracing.TracingUtils.shorten;

public final class MessagingContextManagerImpl extends MessagingContextManager {

    @Override
    @Nullable
    public TraceContext currentTraceContext() {
        return staticCurrentTraceContext();
    }

    @Nullable
    private static TraceContext staticCurrentTraceContext() {
        TraceContextManager currentManager = TraceContextManager.threadContext.get();
        return currentManager != null ? currentManager.getContext() : null;
    }

    @Override
    @Nullable
    public MessageHandlingContext currentMessageHandlingContext() {
        MessageHandlingContextManager currentManager =
                MessageHandlingContextManager.threadContext.get();
        return currentManager != null ? currentManager.getContext() : null;
    }

    @Override
    @Nullable
    public CreationContext currentCreationContext() {
        return staticCurrentCreationContext();
    }

    @Nullable
    private static CreationContext staticCurrentCreationContext() {
        CreationContextManager currentManager = CreationContextManager.threadContext.get();
        return currentManager != null ? currentManager.getContext() : null;
    }

    @Nullable
    @Override
    public CreationContext creationContextFromScope() {
        MessageHandlingContext messageHandlingContext = currentMessageHandlingContext();
        if (messageHandlingContext != null) {
            return new CreationContext(
                    messageHandlingContext.getReceiver(),
                    messageHandlingContext.getReceiverType(),
                    currentMethodContext());
        } else {
            return currentCreationContext();
        }
    }

    @Override
    @Nullable
    public Method currentMethodContext() {
        MethodContextManager currentManager = MethodContextManager.threadContext.get();
        return currentManager != null ? currentManager.getContext() : null;
    }

    @Override
    @Nonnull
    public MessagingScope enter(
            @Nullable ActorContext context,
            @Nullable TracedMessage message) {
        try {
            MessagingScope messagingScope = new MessagingScopeImpl(
                    MessageHandlingContextManager.enter(new MessageHandlingContext(
                            context,
                            message)),
                    TraceContextManager.replace(new TraceContext(message != null
                            ? message.getTraceContext()
                            : null)),
                    message != null && message.getCreationContext() != null
                            ?
                            (CreationContextManager.threadContext.get() != null
                                    ? CreationContextManager.replace(message.getCreationContext())
                                    : CreationContextManager.enter(message.getCreationContext()))
                            : null);
            logger.debug("Entering {}", messagingScope);
            return messagingScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        }
    }

    @Override
    @Nonnull
    public MessagingScope enter(
            @Nullable TraceContext traceContext,
            @Nullable CreationContext creationContext) {
        try {
            MessagingScope messagingScope = new MessagingScopeImpl(
                    traceContext != null ? TraceContextManager.enter(traceContext) : null,
                    creationContext != null ? CreationContextManager.enter(creationContext) : null);
            logger.debug("Entering {}", messagingScope);
            return messagingScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        }
    }

    @Override
    @Nonnull
    public MessagingScope enter(@Nullable TracedMessage message) {
        try {
            MessagingScope messagingScope = new MessagingScopeImpl(
                    TraceContextManager.replace(new TraceContext(message != null
                            ? message.getTraceContext()
                            : null)),
                    message != null && message.getCreationContext() != null
                            ?
                            (CreationContextManager.threadContext.get() != null
                                    ? CreationContextManager.replace(message.getCreationContext())
                                    : CreationContextManager.enter(message.getCreationContext()))
                            : null);
            logger.debug("Entering {}", messagingScope);
            return messagingScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        }
    }

    @Override
    @Nonnull
    public MessagingScope enter(@Nonnull Method context) {
        try {
            MessagingScope messagingScope =
                    new MessagingScopeImpl(MethodContextManager.enter(context));
            logger.debug("Entering {}", messagingScope);
            return messagingScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        }
    }

    private interface ContextManager extends AutoCloseable {

        @Nonnull
        Object getContext();
    }

    public final static class MessagingScopeImpl implements MessagingScope {

        private final ContextManager[] contextManagers;
        private final TraceContext traceContext;
        private final CreationContext creationContext;
        private final AtomicBoolean closed;

        @Override
        @Nullable
        public TraceContext getTraceContext() {
            return traceContext;
        }

        @Nullable
        @Override
        public CreationContext getCreationContext() {
            return creationContext;
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        public MessagingScopeImpl(@Nonnull ContextManager... contextManagers) {
            this.contextManagers = Objects.requireNonNull(contextManagers);
            this.traceContext = find(
                    TraceContext.class,
                    MessagingContextManagerImpl::staticCurrentTraceContext,
                    contextManagers);
            this.creationContext = find(
                    CreationContext.class,
                    MessagingContextManagerImpl::staticCurrentCreationContext,
                    contextManagers);
            this.closed = new AtomicBoolean(false);
        }

        @Nullable
        private <T> T find(
                @Nonnull Class<T> tClass,
                @Nonnull Supplier<T> defaultSupplier,
                @Nonnull ContextManager[] contextManagers) {
            for (ContextManager cm : contextManagers) {
                if (cm != null && tClass.isInstance(cm.getContext())) {
                    return tClass.cast(cm.getContext());
                }
            }
            return defaultSupplier.get();
        }

        @Override
        public void close() {
            closed.set(true);
            logger.debug("Closing {}", this);
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

        @Override
        public String toString() {
            return new StringJoiner(", ", MessagingScope.class.getSimpleName() + "{", "}")
                    .add("contextManagers=" + Arrays.toString(contextManagers))
                    .toString();
        }
    }

    private final static class TraceContextManager implements ContextManager {

        private static final ThreadLocal<TraceContextManager> threadContext = new ThreadLocal<>();

        private enum Strategy {
            ENTER,
            REPLACE
        }

        private final TraceContext context;
        private final TraceContextManager previousManager;
        private final Strategy strategy;

        private TraceContextManager(@Nonnull TraceContext context, @Nonnull Strategy strategy) {
            this.context = Objects.requireNonNull(context);
            this.previousManager = threadContext.get();
            this.strategy = Objects.requireNonNull(strategy);
        }

        @Nonnull
        @Override
        public TraceContext getContext() {
            return context;
        }

        @Nonnull
        private static TraceContextManager enter(@Nonnull TraceContext context) {
            TraceContextManager newManager = new TraceContextManager(context, Strategy.ENTER);
            fillContext(context);
            logEnter(threadContext, newManager);
            threadContext.set(newManager);
            return newManager;
        }

        @Nonnull
        private static TraceContextManager replace(@Nonnull TraceContext context) {
            TraceContextManager newManager = new TraceContextManager(context, Strategy.REPLACE);
            logger.trace("Putting {} in scope", newManager.getContext());
            if (newManager.previousManager == null) {
                logger.error(
                        "Tried to replace a Trace Context with {}, but none is active",
                        context);
            }
            fillContext(context);
            threadContext.set(newManager);
            return newManager;
        }

        @Override
        public void close() {
            logClose(threadContext, this);
            if (previousManager != null) {
                fillContext(previousManager.getContext());
                threadContext.set(previousManager);
            } else {
                clearContext();
                threadContext.remove();
            }
        }

        private static void fillContext(@Nullable TraceContext previous) {
            addToLogContext(SPAN_ID_KEY, previous, TraceContext::getSpanId);
            addToLogContext(TRACE_ID_KEY, previous, TraceContext::getTraceId);
            addToLogContext(PARENT_SPAN_ID_KEY, previous, TraceContext::getParentId);
        }

        private static void clearContext() {
            fillContext(null);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", TraceContextManager.class.getSimpleName() + "{", "}")
                    .add("context=" + context)
                    .add("strategy=" + strategy)
                    .toString();
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
            fillContext(context);
            logEnter(threadContext, newManager);
            threadContext.set(newManager);
            return newManager;
        }

        @Override
        public void close() {
            logClose(threadContext, this);
            clearContext();
            threadContext.remove();
        }

        private static void fillContext(@Nullable MessageHandlingContext context) {
            addToLogContext(MESSAGE_TYPE_KEY, context, MessageHandlingContext::getMessageType);
            addToLogContext(SENDER_KEY, context, MessageHandlingContext::getSender);
            addToLogContext(RECEIVER_KEY, context, MessageHandlingContext::getReceiver);
            addToLogContext(RECEIVER_TYPE_KEY, context, MessageHandlingContext::getReceiverType);
        }

        private static void clearContext() {
            fillContext(null);
        }

        @Override
        public String toString() {
            return new StringJoiner(
                    ", ",
                    MessageHandlingContextManager.class.getSimpleName() + "{",
                    "}")
                    .add("context=" + context)
                    .toString();
        }
    }

    private final static class CreationContextManager implements ContextManager {

        private static final ThreadLocal<CreationContextManager> threadContext =
                new ThreadLocal<>();

        private enum Strategy {
            ENTER,
            REPLACE
        }

        private final CreationContext context;
        private final CreationContextManager previousManager;
        private final Strategy strategy;

        @Nonnull
        @Override
        public CreationContext getContext() {
            return context;
        }

        private CreationContextManager(
                @Nonnull CreationContext context,
                @Nonnull Strategy strategy) {
            this.context = Objects.requireNonNull(context);
            this.previousManager = threadContext.get();
            this.strategy = Objects.requireNonNull(strategy);
        }

        @Nonnull
        private static CreationContextManager enter(@Nonnull CreationContext context) {
            CreationContextManager newManager = new CreationContextManager(context, Strategy.ENTER);
            fillContext(context);
            logEnter(threadContext, newManager);
            threadContext.set(newManager);
            return newManager;
        }

        @Nonnull
        private static CreationContextManager replace(@Nonnull CreationContext context) {
            CreationContextManager newManager =
                    new CreationContextManager(context, Strategy.REPLACE);
            logger.trace("Putting {} in scope", newManager.getContext());
            if (newManager.previousManager == null) {
                logger.error(
                        "Tried to replace a Creation Context with {}, but none is active",
                        context);
            }
            fillContext(context);
            threadContext.set(newManager);
            return newManager;
        }

        @Override
        public void close() {
            logClose(threadContext, this);
            if (previousManager != null) {
                fillContext(previousManager.getContext());
                threadContext.set(previousManager);
            } else {
                clearContext();
                threadContext.remove();
            }
        }

        private static void clearContext() {
            fillContext(null);
        }

        private static void fillContext(@Nullable CreationContext context) {
            addToLogContext(CREATOR_KEY, context, CreationContext::getCreator);
            addToLogContext(CREATOR_TYPE_KEY, context, CreationContext::getCreatorType);
            addToLogContext(CREATOR_METHOD_KEY, context, CreationContext::getCreatorMethod);
            addToLogContext(SCHEDULED_KEY, context, CreationContext::getScheduled);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", CreationContextManager.class.getSimpleName() + "{", "}")
                    .add("context=" + context)
                    .add("previousManager=" + previousManager)
                    .add("strategy=" + strategy)
                    .toString();
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
            fillContext(context);
            logEnter(threadContext, newManager);
            threadContext.set(newManager);
            return newManager;
        }

        private static void fillContext(@Nullable Method context) {
            addToLogContext(RECEIVER_METHOD_KEY, context, TracingUtils::shorten);
        }

        private static void clearContext() {
            fillContext(null);
        }

        @Override
        public void close() {
            logClose(threadContext, this);
            clearContext();
            threadContext.remove();
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", MethodContextManager.class.getSimpleName() + "{", "}")
                    .add("context=" + shorten(context))
                    .toString();
        }
    }

    private static <T extends ContextManager> void logEnter(
            @Nonnull ThreadLocal<T> threadContext,
            @Nonnull T contextManager) {
        logger.trace("Putting {} in scope", contextManager);
        T current = threadContext.get();
        if (current != null) {
            logger.error(
                    "Putting {} in scope, but {} is already in scope. "
                            + "Context in scope likely incorrect.",
                    contextManager,
                    current);
        }
    }

    private static <T extends ContextManager> void logClose(
            @Nonnull ThreadLocal<T> threadContext,
            @Nonnull T closingScope) {
        logger.trace("Removing {} from scope", closingScope.getContext());
        T current = threadContext.get();
        if (current != closingScope) {
            logger.error(
                    "Removing {} from scope, but context in scope was actually {}. "
                            + "Context in scope likely incorrect.",
                    closingScope.getContext(),
                    current != null ? current.getContext() : null);
        }
    }

    private static <D, T> void addToLogContext(
            @Nonnull String key,
            @Nullable D object,
            @Nonnull Function<D, T> getterFunction) {
        String value = object == null ? null : safeToString(getterFunction.apply(object));
        if (value != null) {
            MDC.put(key, value);
        } else {
            MDC.remove(key);
        }
    }

}
