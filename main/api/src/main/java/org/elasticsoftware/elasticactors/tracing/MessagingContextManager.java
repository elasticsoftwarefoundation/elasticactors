/*
 * Copyright 2013 - 2024 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.tracing;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.elasticsoftware.elasticactors.ActorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Optional;
import java.util.ServiceLoader;

public abstract class MessagingContextManager {

    protected final static Logger logger = LoggerFactory.getLogger(MessagingContextManager.class);

    /*
     * MDC keys
     */

    // Tracing headers
    public static final String SPAN_ID_KEY = "spanId";
    public static final String TRACE_ID_KEY = "traceId";
    public static final String PARENT_SPAN_ID_KEY = "parentId";

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

    @Nonnull
    public static MessagingContextManager getManager() {
        return MessagingContextManagerHolder.INSTANCE;
    }

    public interface MessagingScope extends AutoCloseable {

        @Nullable
        TraceContext getTraceContext();

        @Nullable
        CreationContext getCreationContext();

        @Nullable
        CreationContext creationContextFromScope();

        @Nullable
        MessageHandlingContext getMessageHandlingContext();

        @Nullable
        Method getMethod();

        boolean isClosed();

        @Override
        void close();
    }

    public abstract boolean isTracingEnabled();

    /**
     * Retrieves the current context for the current thread.
     * The implementations will likely use a ThreadLocal for this.
     * It's therefore recommended to only call this method once and reuse the object as much as possible.
     */
    @Nullable
    public abstract MessagingScope currentScope();

    @Nonnull
    public abstract MessagingScope enter(
            @Nullable ActorContext context,
            @Nullable TracedMessage message);

    @Nonnull
    public abstract MessagingScope enter(
            @Nullable TraceContext traceContext,
            @Nullable CreationContext creationContext);

    @Nonnull
    public abstract MessagingScope enter(@Nullable TracedMessage message);

    @Nonnull
    public abstract MessagingScope enter(@Nonnull Method context);

    public abstract boolean isLogContextProcessingEnabled();

    /*
     * Initialization-on-deman holder pattern (lazy-loaded singleton)
     * See: https://en.wikipedia.org/wiki/Initialization-on-demand_holder_idiom
     */
    private static final class MessagingContextManagerHolder {

        private static final MessagingContextManager INSTANCE = loadService();

        private static MessagingContextManager loadService() {
            try {
                return Optional.of(ServiceLoader.load(MessagingContextManager.class))
                        .map(ServiceLoader::iterator)
                        .filter(Iterator::hasNext)
                        .map(MessagingContextManagerHolder::loadFirst)
                        .orElseGet(() -> {
                            logger.warn(
                                    "No implementations of MessagingContextManager were found. "
                                            + "Falling back to no-op. Tracing disabled.");
                            return new NoopMessagingContextManager();
                        });
            } catch (Exception e) {
                logger.error(
                        "Exception thrown while loading MessagingContextManager implementation. "
                                + "Falling back to no-op. Tracing disabled.", e);
                return new NoopMessagingContextManager();
            }
        }

        private static MessagingContextManager loadFirst(Iterator<MessagingContextManager> iter) {
            MessagingContextManager service = iter.next();
            logger.info(
                "Loaded MessagingContextManager implementation [{}]. Tracing enabled: {}",
                service.getClass().getName(),
                service.isTracingEnabled()
            );
            return service;
        }
    }

    /**
     * NO-OP implementation for when an implementation of the manager cannot be found
     */
    private final static class NoopMessagingContextManager extends MessagingContextManager {

        @Override
        public boolean isTracingEnabled() {
            return false;
        }

        @Nullable
        @Override
        public MessagingScope currentScope() {
            return null;
        }

        @Nonnull
        @Override
        public MessagingScope enter(
                @Nullable ActorContext context,
                @Nullable TracedMessage message) {
            return NoopMessagingScope.INSTANCE;
        }

        @Nonnull
        @Override
        public MessagingScope enter(
                @Nullable TraceContext traceContext,
                @Nullable CreationContext creationContext) {
            return NoopMessagingScope.INSTANCE;
        }

        @Nonnull
        @Override
        public MessagingScope enter(@Nullable TracedMessage message) {
            return NoopMessagingScope.INSTANCE;
        }

        @Nonnull
        @Override
        public MessagingScope enter(@Nonnull Method context) {
            return NoopMessagingScope.INSTANCE;
        }

        @Override
        public boolean isLogContextProcessingEnabled() {
            return false;
        }
    }

}
