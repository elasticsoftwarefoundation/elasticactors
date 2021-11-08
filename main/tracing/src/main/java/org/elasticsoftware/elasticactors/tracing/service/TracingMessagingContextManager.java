package org.elasticsoftware.elasticactors.tracing.service;

import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.LogContextProcessor;
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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.elasticsoftware.elasticactors.tracing.TracingUtils.safeToString;

import static java.lang.ThreadLocal.withInitial;

public final class TracingMessagingContextManager extends MessagingContextManager {

    private final static ThreadLocal<Deque<MessagingScope>> scopes = withInitial(ArrayDeque::new);

    /*
     * Initialization-on-deman holder pattern (lazy-loaded singleton)
     * See: https://en.wikipedia.org/wiki/Initialization-on-demand_holder_idiom
     */
    private static final class LogContextProcessorHolder {

        private static final LogContextProcessor INSTANCE = loadService();

        private static LogContextProcessor loadService() {
            try {
                return Optional.of(ServiceLoader.load(LogContextProcessor.class))
                    .map(ServiceLoader::iterator)
                    .filter(Iterator::hasNext)
                    .map(LogContextProcessorHolder::loadFirst)
                    .orElseGet(() -> {
                        logger.warn(
                            "No implementations of LogContextProcessor were found. "
                                + "Falling back to the unoptimized default.");
                        return new Slf4jMDCLogContexProcessor();
                    });
            } catch (Exception e) {
                logger.error(
                    "Exception thrown while loading LogContextProcessor implementation. "
                        + "Falling back to the unoptimized default.", e);
                return new Slf4jMDCLogContexProcessor();
            }
        }

        private static LogContextProcessor loadFirst(Iterator<LogContextProcessor> iter) {
            LogContextProcessor service = iter.next();
            logger.info(
                "Loaded LogContextProcessor implementation [{}]",
                service.getClass().getName()
            );
            return service;
        }
    }

    @Override
    public boolean isTracingEnabled() {
        return true;
    }

    @Nullable
    @Override
    public MessagingScope currentScope() {
        return scopes.get().peek();
    }

    @Override
    @Nonnull
    public MessagingScope enter(
        @Nullable ActorContext context,
        @Nullable TracedMessage message)
    {
        try {
            Deque<MessagingScope> currentStack = scopes.get();
            MessagingScope currentScope = currentStack.peek();
            MessagingScope newScope = new TracingMessagingScope(
                new TraceContext(message != null
                    ? message.getTraceContext()
                    : null),
                message != null
                    ? message.getCreationContext()
                    : currentScope != null
                        ? currentScope.getCreationContext()
                        : null,
                new MessageHandlingContext(context, message),
                currentScope != null ? currentScope.getMethod() : null
            );
            enterScope(currentStack, currentScope, newScope);
            return newScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        }
    }

    @Override
    @Nonnull
    public MessagingScope enter(
        @Nullable TraceContext traceContext,
        @Nullable CreationContext creationContext)
    {
        try {
            Deque<MessagingScope> currentStack = scopes.get();
            MessagingScope currentScope = currentStack.peek();
            MessagingScope newScope = new TracingMessagingScope(
                traceContext,
                creationContext,
                currentScope != null ? currentScope.getMessageHandlingContext() : null,
                currentScope != null ? currentScope.getMethod() : null
            );
            enterScope(currentStack, currentScope, newScope);
            return newScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        }
    }

    @Override
    @Nonnull
    public MessagingScope enter(@Nullable TracedMessage message) {
        try {
            Deque<MessagingScope> currentStack = scopes.get();
            MessagingScope currentScope = currentStack.peek();
            MessagingScope newScope = new TracingMessagingScope(
                new TraceContext(message != null
                    ? message.getTraceContext()
                    : null),
                message != null
                    ? message.getCreationContext()
                    : currentScope != null
                        ? currentScope.getCreationContext()
                        : null,
                currentScope != null ? currentScope.getMessageHandlingContext() : null,
                currentScope != null ? currentScope.getMethod() : null
            );
            enterScope(currentStack, currentScope, newScope);
            return newScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        }
    }

    @Override
    @Nonnull
    public MessagingScope enter(@Nonnull Method context) {
        try {
            Deque<MessagingScope> currentStack = scopes.get();
            MessagingScope currentScope = currentStack.peek();
            MessagingScope newScope = new TracingMessagingScope(
                currentScope != null ? currentScope.getTraceContext() : null,
                currentScope != null ? currentScope.getCreationContext() : null,
                currentScope != null ? currentScope.getMessageHandlingContext() : null,
                context
            );
            enterScope(currentStack, currentScope, newScope);
            return newScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        }
    }

    private static void enterScope(
        Deque<MessagingScope> currentStack,
        MessagingScope currentScope,
        MessagingScope newScope)
    {
        logger.debug("Current number of stacked scopes: {}", currentStack.size());
        logger.debug("Current scope: {}", currentScope);
        currentStack.push(newScope);
        fillContext(currentScope, newScope);
        logger.debug("Entering new scope: {}", newScope);
    }

    private final static class TracingMessagingScope implements MessagingScope {

        private final TraceContext traceContext;
        private final CreationContext creationContext;
        private final MessageHandlingContext messageHandlingContext;
        private final Method method;
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

        @Nullable
        @Override
        public CreationContext creationContextFromScope() {
            if (messageHandlingContext != null) {
                return new CreationContext(
                    messageHandlingContext.getReceiver(),
                    messageHandlingContext.getReceiverType(),
                    method
                );
            } else {
                return creationContext;
            }
        }

        @Nullable
        @Override
        public MessageHandlingContext getMessageHandlingContext() {
            return messageHandlingContext;
        }

        @Nullable
        @Override
        public Method getMethod() {
            return method;
        }


        @Override
        public boolean isClosed() {
            return closed.get();
        }

        public TracingMessagingScope(
            @Nullable TraceContext traceContext,
            @Nullable CreationContext creationContext,
            @Nullable MessageHandlingContext messageHandlingContext,
            @Nullable Method method)
        {
            this.traceContext = traceContext;
            this.creationContext = creationContext;
            this.messageHandlingContext = messageHandlingContext;
            this.method = method;
            this.closed = new AtomicBoolean(false);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                logger.debug("Closing scope: {}", this);
                Deque<MessagingScope> currentStack = scopes.get();
                logger.debug("Current number of stacked scopes: {}", currentStack.size());
                MessagingScope currentScope = currentStack.peek();
                if (currentScope == this) {
                    currentStack.pop();
                    MessagingScope previousScope = currentStack.peek();
                    logger.debug("Number of scopes left in the stack: {}", currentStack.size());
                    logger.debug("Rolling back to scope: {}", previousScope);
                    fillContext(this, previousScope);
                } else {
                    logger.error(
                        "Removing {} from scope, but context in scope was actually {}. "
                            + "Context in scope likely incorrect.",
                        this,
                        currentScope
                    );
                }
            } else {
                logger.warn("Trying to close on an already closed scope: {}", this);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TracingMessagingScope)) {
                return false;
            }
            TracingMessagingScope that = (TracingMessagingScope) o;
            return Objects.equals(traceContext, that.traceContext)
                && Objects.equals(creationContext, that.creationContext)
                && Objects.equals(messageHandlingContext, that.messageHandlingContext)
                && Objects.equals(method, that.method);
        }

        @Override
        public int hashCode() {
            return Objects.hash(traceContext, creationContext, messageHandlingContext, method);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", TracingMessagingScope.class.getSimpleName() + "{", "}")
                .add("traceContext=" + traceContext)
                .add("creationContext=" + creationContext)
                .add("messageHandlingContext=" + messageHandlingContext)
                .add("method=" + method)
                .add("closed=" + closed)
                .toString();
        }
    }

    private static void fillContext(
        @Nullable MessagingScope current,
        @Nullable MessagingScope next)
    {
        LogContextProcessorHolder.INSTANCE.process(current, next);
    }

    private final static class Slf4jMDCLogContexProcessor implements LogContextProcessor {

        @Override
        public void process(@Nullable MessagingScope current, @Nullable MessagingScope next) {
            TraceContext currentTraceContext = null;
            CreationContext currentCreationContext = null;
            MessageHandlingContext currentMessagingContext = null;
            Method currentMethod = null;

            TraceContext nextTraceContext = null;
            CreationContext nextCreationContext = null;
            MessageHandlingContext nextMessagingContext = null;
            Method nextMethod = null;

            if (current != null) {
                currentTraceContext = current.getTraceContext();
                currentCreationContext = current.getCreationContext();
                currentMessagingContext = current.getMessageHandlingContext();
                currentMethod = current.getMethod();
            }

            if (next != null) {
                nextTraceContext = next.getTraceContext();
                nextCreationContext = next.getCreationContext();
                nextMessagingContext = next.getMessageHandlingContext();
                nextMethod = next.getMethod();
            }

            if (currentTraceContext != nextTraceContext) {
                fillContext(currentTraceContext, nextTraceContext);
            }
            if (currentCreationContext != nextCreationContext) {
                fillContext(currentCreationContext, nextCreationContext);
            }
            if (currentMessagingContext != nextMessagingContext) {
                fillContext(currentMessagingContext, nextMessagingContext);
            }
            if (currentMethod != nextMethod) {
                fillContext(nextMethod);
            }
        }

        private static void fillContext(@Nullable TraceContext current, @Nullable TraceContext next) {
            updateBaggage(current, next);
            addToLogContext(SPAN_ID_KEY, current, next, TraceContext::getSpanId);
            addToLogContext(TRACE_ID_KEY, current, next, TraceContext::getTraceId);
            addToLogContext(PARENT_SPAN_ID_KEY, current, next, TraceContext::getParentId);
        }

        private static void updateBaggage(TraceContext current, TraceContext next) {
            try {
                Map<String, String> oldBaggage = current != null ? current.getBaggage() : null;
                Map<String, String> nextBaggage = next != null ? next.getBaggage() : null;
                if (oldBaggage == nextBaggage) {
                    return;
                }
                if (oldBaggage == null) {
                    nextBaggage.forEach(Slf4jMDCLogContexProcessor::putOnMDC);
                } else if (nextBaggage == null) {
                    oldBaggage.keySet().forEach(key -> putOnMDC(key, null));
                } else {
                    oldBaggage.forEach((key, value) -> {
                        String nextValue = nextBaggage.get(key);
                        // Will set the new value if present, and remove it if absent
                        if (!Objects.equals(value, nextValue)) {
                            putOnMDC(key, nextValue);
                        }
                    });
                    nextBaggage.forEach((key, value) -> {
                        String oldValue = oldBaggage.get(key);
                        // Would have been processed when processing oldBaggage
                        if (oldValue != null) {
                            return;
                        }
                        // Only insert if the old value was null and the new one isn't
                        if (value != null) {
                            putOnMDC(key, value);
                        }
                    });
                }
            } catch (Exception e) {
                logger.error(
                    "Could not add trace baggage to the MDC. "
                        + "Baggage fields on the MDC might be corrupted.",
                    e
                );
            }
        }

        private static void fillContext(
            @Nullable MessageHandlingContext current,
            @Nullable MessageHandlingContext next)
        {
            addToLogContext(MESSAGE_TYPE_KEY, current, next, MessageHandlingContext::getMessageType);
            addToLogContext(SENDER_KEY, current, next, MessageHandlingContext::getSender);
            addToLogContext(RECEIVER_KEY, current, next, MessageHandlingContext::getReceiver);
            addToLogContext(RECEIVER_TYPE_KEY, current, next, MessageHandlingContext::getReceiverType);
        }

        private static void fillContext(
            @Nullable CreationContext current,
            @Nullable CreationContext next)
        {
            addToLogContext(CREATOR_KEY, current, next, CreationContext::getCreator);
            addToLogContext(CREATOR_TYPE_KEY, current, next, CreationContext::getCreatorType);
            addToLogContext(CREATOR_METHOD_KEY, current, next, CreationContext::getCreatorMethod);
            addToLogContext(SCHEDULED_KEY, current, next, CreationContext::getScheduled);
        }

        private static void fillContext(@Nullable Method next) {
            putOnMDC(RECEIVER_METHOD_KEY, getValue(next, TracingUtils::shorten));
        }

        private static <D, T> void addToLogContext(
            @Nonnull String key,
            @Nullable D oldObject,
            @Nullable D newObject,
            @Nonnull Function<D, T> getterFunction)
        {
            String oldValue = getValue(oldObject, getterFunction);
            String newValue = getValue(newObject, getterFunction);
            if (!(Objects.equals(oldValue, newValue))) {
                putOnMDC(key, newValue);
            }
        }

        private static void putOnMDC(@Nonnull String key, String newValue) {
            if (newValue != null) {
                MDC.put(key, newValue);
            } else {
                MDC.remove(key);
            }
        }

        private static <D, T> String getValue(
            @Nullable D oldObject,
            @Nonnull Function<D, T> getterFunction)
        {
            return oldObject != null ? safeToString(getterFunction.apply(oldObject)) : null;
        }
    }
}
