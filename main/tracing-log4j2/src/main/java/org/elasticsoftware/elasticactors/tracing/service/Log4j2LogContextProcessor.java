package org.elasticsoftware.elasticactors.tracing.service;

import org.elasticsoftware.elasticactors.tracing.LogContextProcessor;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;

import javax.annotation.Nullable;

public final class Log4j2LogContextProcessor implements LogContextProcessor {

    @Override
    public void process(@Nullable MessagingScope current, @Nullable MessagingScope next) {

    }
}
