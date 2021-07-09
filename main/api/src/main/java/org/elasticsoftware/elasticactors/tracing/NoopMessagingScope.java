package org.elasticsoftware.elasticactors.tracing;

import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;

import javax.annotation.Nullable;
import java.util.StringJoiner;

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

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return new StringJoiner(
                ", ",
                NoopMessagingScope.class.getSimpleName() + "{",
                "}").toString();
    }
}