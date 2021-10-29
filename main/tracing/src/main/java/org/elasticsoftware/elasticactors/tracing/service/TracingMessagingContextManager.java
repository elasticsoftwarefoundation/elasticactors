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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.elasticsoftware.elasticactors.tracing.TracingUtils.safeToString;

import static java.lang.ThreadLocal.withInitial;

public final class TracingMessagingContextManager extends MessagingContextManager {

    private final static ThreadLocal<Deque<MessagingScope>> scopes = withInitial(ArrayDeque::new);

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
            enter(currentStack, currentScope, newScope);
            logger.debug("Entering {}", newScope);
            return newScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        }
    }

    private static void enter(
        Deque<MessagingScope> currentStack,
        MessagingScope currentScope,
        MessagingScope newScope)
    {
        currentStack.push(newScope);
        fillContext(currentScope, newScope);
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
            enter(currentStack, currentScope, newScope);
            logger.debug("Entering {}", newScope);
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
            enter(currentStack, currentScope, newScope);
            logger.debug("Entering {}", newScope);
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
            enter(currentStack, currentScope, newScope);
            logger.debug("Entering {}", newScope);
            return newScope;
        } catch (Exception e) {
            logger.error("Exception thrown while creating messaging scope", e);
            return NoopMessagingScope.INSTANCE;
        }
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
                logger.debug("Closing {}", this);
                Deque<MessagingScope> currentStack = scopes.get();
                MessagingScope currentScope = currentStack.peek();
                if (currentScope == this) {
                    currentStack.pop();
                    fillContext(this, currentStack.peek());
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
            fillContext(nextTraceContext);
        }
        if (currentCreationContext != nextCreationContext) {
            fillContext(nextCreationContext);
        }
        if (currentMessagingContext != nextMessagingContext) {
            fillContext(nextMessagingContext);
        }
        if (currentMethod != nextMethod) {
            fillContext(nextMethod);
        }
    }

    private static void fillContext(@Nullable TraceContext context) {
        addToLogContext(SPAN_ID_KEY, context, TraceContext::getSpanId);
        addToLogContext(TRACE_ID_KEY, context, TraceContext::getTraceId);
        addToLogContext(PARENT_SPAN_ID_KEY, context, TraceContext::getParentId);
    }

    private static void fillContext(@Nullable MessageHandlingContext context) {
        addToLogContext(MESSAGE_TYPE_KEY, context, MessageHandlingContext::getMessageType);
        addToLogContext(SENDER_KEY, context, MessageHandlingContext::getSender);
        addToLogContext(RECEIVER_KEY, context, MessageHandlingContext::getReceiver);
        addToLogContext(RECEIVER_TYPE_KEY, context, MessageHandlingContext::getReceiverType);
    }

    private static void fillContext(@Nullable CreationContext context) {
        addToLogContext(CREATOR_KEY, context, CreationContext::getCreator);
        addToLogContext(CREATOR_TYPE_KEY, context, CreationContext::getCreatorType);
        addToLogContext(CREATOR_METHOD_KEY, context, CreationContext::getCreatorMethod);
        addToLogContext(SCHEDULED_KEY, context, CreationContext::getScheduled);
    }

    private static void fillContext(@Nullable Method context) {
        addToLogContext(RECEIVER_METHOD_KEY, context, TracingUtils::shorten);
    }

    private static <D, T> void addToLogContext(
        @Nonnull String key,
        @Nullable D object,
        @Nonnull Function<D, T> getterFunction)
    {
        String value = object != null ? safeToString(getterFunction.apply(object)) : null;
        if (value != null) {
            MDC.put(key, value);
        } else {
            MDC.remove(key);
        }
    }
}
