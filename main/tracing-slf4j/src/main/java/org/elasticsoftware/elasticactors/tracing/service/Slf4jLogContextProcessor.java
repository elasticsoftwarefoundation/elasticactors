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

import org.elasticsoftware.elasticactors.tracing.CreationContext;
import org.elasticsoftware.elasticactors.tracing.LogContextProcessor;
import org.elasticsoftware.elasticactors.tracing.MessageHandlingContext;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.tracing.TraceContext;
import org.elasticsoftware.elasticactors.tracing.TracingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.CREATOR_KEY;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.CREATOR_METHOD_KEY;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.CREATOR_TYPE_KEY;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MESSAGE_TYPE_KEY;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.PARENT_SPAN_ID_KEY;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.RECEIVER_KEY;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.RECEIVER_METHOD_KEY;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.RECEIVER_TYPE_KEY;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.SCHEDULED_KEY;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.SENDER_KEY;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.SPAN_ID_KEY;
import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.TRACE_ID_KEY;
import static org.elasticsoftware.elasticactors.tracing.TracingUtils.safeToString;

public final class Slf4jLogContextProcessor implements LogContextProcessor {

    private final static Logger logger = LoggerFactory.getLogger(Slf4jLogContextProcessor.class);

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

    @Override
    public boolean isLogContextProcessingEnabled() {
        return true;
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
                nextBaggage.forEach(Slf4jLogContextProcessor::putOnMDC);
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