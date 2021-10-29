package org.elasticsoftware.elasticactors.tracing;

import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;

import javax.annotation.Nullable;
import java.lang.reflect.Method;

public final class NoopMessagingScope implements MessagingScope {

    public final static NoopMessagingScope INSTANCE = new NoopMessagingScope();

    @Nullable
    @Override
    public TraceContext getTraceContext() {
        return null;
    }

    @Nullable
    @Override
    public CreationContext getCreationContext() {
        return null;
    }

    @Nullable
    @Override
    public CreationContext creationContextFromScope() {
        return null;
    }

    @Nullable
    @Override
    public MessageHandlingContext getMessageHandlingContext() {
        return null;
    }

    @Nullable
    @Override
    public Method getMethod() {
        return null;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return NoopMessagingScope.class.getSimpleName() + "{}";
    }
}