package org.elasticsoftware.elasticactors.tracing.service;

import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.MessageHandlingContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager;
import org.elasticsoftware.elasticactors.tracing.NoopMessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.elasticsoftware.elasticactors.tracing.TracedMessage;
import org.elasticsoftware.elasticactors.tracing.TracingUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.elasticsoftware.elasticactors.tracing.TracingUtils.safeToString;
import static org.elasticsoftware.elasticactors.tracing.TracingUtils.shorten;

public final class MessagingContextManagerImpl extends MessagingContextManager {

    /*
     * Initialization-on-deman holder pattern (lazy-loaded singleton)
     * See: https://en.wikipedia.org/wiki/Initialization-on-demand_holder_idiom
     */
    private static final class DiagnosticContextManagerHolder {

        private static final DiagnosticContext INSTANCE = loadService();

        private static DiagnosticContext loadService() {
            try {
                return Optional.of(ServiceLoader.load(DiagnosticContext.class))
                        .map(ServiceLoader::iterator)
                        .filter(Iterator::hasNext)
                        .map(Iterator::next)
                        .orElseGet(() -> {
                            logger.info(
                                    "No specific implementations of DiagnosticContext were found. "
                                            + "Falling back to the generic SLF4J implementation.");
                            return new Slf4jDiagnosticContext();
                        });
            } catch (Exception e) {
                logger.error(
                        "Exception thrown while loading DiagnosticContext implementation. "
                                + "Falling back to no-op.", e);
                return new Slf4jDiagnosticContext();
            }
        }
    }

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
        DiagnosticContextManagerHolder.INSTANCE.init();
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
                            : null,
                    MessageHandlingContextManager.enter(new MessageHandlingContext(
                            context,
                            message)),
                    null);
            logger.debug("Entering {}", messagingScope);
            return messagingScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        } finally {
            DiagnosticContextManagerHolder.INSTANCE.finish();
        }
    }

    @Override
    @Nonnull
    public MessagingScope enter(
            @Nullable TraceContext traceContext,
            @Nullable CreationContext creationContext) {
        DiagnosticContextManagerHolder.INSTANCE.init();
        try {
            MessagingScope messagingScope = new MessagingScopeImpl(
                    traceContext != null ? TraceContextManager.enter(traceContext) : null,
                    creationContext != null ? CreationContextManager.enter(creationContext) : null,
                    null,
                    null);
            logger.debug("Entering {}", messagingScope);
            return messagingScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        } finally {
            DiagnosticContextManagerHolder.INSTANCE.finish();
        }
    }

    @Override
    @Nonnull
    public MessagingScope enter(@Nullable TracedMessage message) {
        DiagnosticContextManagerHolder.INSTANCE.init();
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
                            : null,
                    null,
                    null);
            logger.debug("Entering {}", messagingScope);
            return messagingScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        } finally {
            DiagnosticContextManagerHolder.INSTANCE.finish();
        }
    }

    @Override
    @Nonnull
    public MessagingScope enter(@Nonnull Method context) {
        DiagnosticContextManagerHolder.INSTANCE.init();
        try {
            MessagingScope messagingScope =
                    new MessagingScopeImpl(null, null, null, MethodContextManager.enter(context));
            logger.debug("Entering {}", messagingScope);
            return messagingScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        } finally {
            DiagnosticContextManagerHolder.INSTANCE.finish();
        }
    }

    public final static class MessagingScopeImpl implements MessagingScope {

        private final TraceContextManager traceContextManager;
        private final CreationContextManager creationContextManager;
        private final MessageHandlingContextManager messageHandlingContextManager;
        private final MethodContextManager methodContextManager;

        private final TraceContext currentTraceContext;
        private final CreationContext currentCreationContext;
        private final AtomicBoolean closed;

        @Override
        @Nullable
        public TraceContext getTraceContext() {
            return currentTraceContext;
        }

        @Nullable
        @Override
        public CreationContext getCreationContext() {
            return currentCreationContext;
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        public MessagingScopeImpl(
                @Nullable TraceContextManager traceContextManager,
                @Nullable CreationContextManager creationContextManager,
                @Nullable MessageHandlingContextManager messageHandlingContextManager,
                @Nullable MethodContextManager methodContextManager) {
            this.traceContextManager = traceContextManager;
            this.creationContextManager = creationContextManager;
            this.messageHandlingContextManager = messageHandlingContextManager;
            this.methodContextManager = methodContextManager;
            this.currentTraceContext = traceContextManager != null
                    ? traceContextManager.getContext()
                    : staticCurrentTraceContext();
            this.currentCreationContext = creationContextManager != null
                    ? creationContextManager.getContext()
                    : staticCurrentCreationContext();
            this.closed = new AtomicBoolean();
        }

        @Override
        public void close() {
            if (closed.getAndSet(true)) {
                return;
            }
            logger.debug("Closing {}", this);
            DiagnosticContextManagerHolder.INSTANCE.init();
            closeSafely(traceContextManager);
            closeSafely(creationContextManager);
            closeSafely(messageHandlingContextManager);
            closeSafely(methodContextManager);
            DiagnosticContextManagerHolder.INSTANCE.finish();
        }

        private static void closeSafely(@Nullable AutoCloseable object) {
            try {
                if (object != null) {
                    object.close();
                }
            } catch (Exception e) {
                logger.error("Exception thrown while closing {}", object, e);
            }
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", MessagingScopeImpl.class.getSimpleName() + "{", "}")
                    .add("traceContextManager=" + traceContextManager)
                    .add("creationContextManager=" + creationContextManager)
                    .add("messageHandlingContextManager=" + messageHandlingContextManager)
                    .add("methodContextManager=" + methodContextManager)
                    .add("currentTraceContext=" + currentTraceContext)
                    .add("currentCreationContext=" + currentCreationContext)
                    .add("closed=" + closed)
                    .toString();
        }
    }

    private final static class TraceContextManager implements AutoCloseable {

        private static final ThreadLocal<TraceContextManager> threadContext = new ThreadLocal<>();

        private enum Strategy {
            ENTER,
            REPLACE
        }

        private final TraceContext context;
        private final TraceContextManager previousManager;
        private final Strategy strategy;

        @Nonnull
        public TraceContext getContext() {
            return context;
        }

        private TraceContextManager(@Nonnull TraceContext context, @Nonnull Strategy strategy) {
            this.context = Objects.requireNonNull(context);
            this.previousManager = threadContext.get();
            this.strategy = Objects.requireNonNull(strategy);
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
            logger.trace("Putting {} in scope", newManager);
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

        private static void fillContext(@Nullable TraceContext context) {
            addToLogContext(SPAN_ID_KEY, context, TraceContext::getSpanId);
            addToLogContext(TRACE_ID_KEY, context, TraceContext::getTraceId);
            addToLogContext(PARENT_SPAN_ID_KEY, context, TraceContext::getParentId);
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

    private final static class MessageHandlingContextManager implements AutoCloseable {

        private static final ThreadLocal<MessageHandlingContextManager> threadContext =
                new ThreadLocal<>();

        private final MessageHandlingContext context;

        private MessageHandlingContextManager(@Nonnull MessageHandlingContext context) {
            this.context = Objects.requireNonNull(context);
        }

        @Nonnull
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

    private final static class CreationContextManager implements AutoCloseable {

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
            logger.trace("Putting {} in scope", newManager);
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

        private static void fillContext(@Nullable CreationContext context) {
            addToLogContext(CREATOR_KEY, context, CreationContext::getCreator);
            addToLogContext(CREATOR_TYPE_KEY, context, CreationContext::getCreatorType);
            addToLogContext(CREATOR_METHOD_KEY, context, CreationContext::getCreatorMethod);
            addToLogContext(SCHEDULED_KEY, context, CreationContext::getScheduled);
        }

        private static void clearContext() {
            fillContext(null);
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

    private final static class MethodContextManager implements AutoCloseable {

        private static final ThreadLocal<MethodContextManager> threadContext = new ThreadLocal<>();

        private final Method context;

        private MethodContextManager(@Nonnull Method context) {
            this.context = Objects.requireNonNull(context);
        }

        @Nonnull
        public Method getContext() {
            return context;
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

    private static <T> void logEnter(
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

    private static <T> void logClose(
            @Nonnull ThreadLocal<T> threadContext,
            @Nonnull T contextManager) {
        logger.trace("Removing {} from scope", contextManager);
        T current = threadContext.get();
        if (current != contextManager) {
            logger.error(
                    "Removing {} from scope, but context in scope was actually {}. "
                            + "Context in scope likely incorrect.",
                    contextManager,
                    current);
        }
    }

    private static <D, T> void addToLogContext(
            @Nonnull String key,
            @Nullable D object,
            @Nonnull Function<D, T> getterFunction) {
        String value = object != null ? safeToString(getterFunction.apply(object)) : null;
        DiagnosticContextManagerHolder.INSTANCE.put(key, value);
    }

}
