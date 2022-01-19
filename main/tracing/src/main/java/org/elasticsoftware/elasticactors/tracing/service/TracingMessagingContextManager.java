/*
 *   Copyright 2013 - 2022 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.tracing.service;

import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.LogContextProcessor;
import org.elasticsoftware.elasticactors.tracing.MessageHandlingContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager;
import org.elasticsoftware.elasticactors.tracing.NoopMessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.elasticsoftware.elasticactors.tracing.TracedMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;

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
                                + "Falling back to no-op.");
                        return new NoopLogContextProcessor();
                    });
            } catch (Exception e) {
                logger.error(
                    "Exception thrown while loading LogContextProcessor implementation. "
                        + "Falling back to no-op.", e);
                return new NoopLogContextProcessor();
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

    @Override
    public boolean isLogContextProcessingEnabled() {
        return LogContextProcessorHolder.INSTANCE.isLogContextProcessingEnabled();
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

    /**
     * NO-OP implementation for when an implementation of the log context processor cannot be found
     */
    private static final class NoopLogContextProcessor implements LogContextProcessor {

        @Override
        public void process(@Nullable MessagingScope current, @Nullable MessagingScope next) {
            // do nothing
        }

        @Override
        public boolean isLogContextProcessingEnabled() {
            return false;
        }
    }
}
