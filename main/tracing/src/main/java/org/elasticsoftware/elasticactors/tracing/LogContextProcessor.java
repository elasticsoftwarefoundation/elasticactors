package org.elasticsoftware.elasticactors.tracing;

import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;

import javax.annotation.Nullable;

public interface LogContextProcessor {

    void process(@Nullable MessagingScope current, @Nullable MessagingScope next);
}
