package org.elasticsoftware.elasticactors.tracing.service;

import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

final class Slf4jDiagnosticContext implements DiagnosticContext {

    @Override
    public void init() {
        // Do nothing
    }

    @Override
    public void put(@Nonnull String key, @Nullable String value) {
        if (value != null) {
            MDC.put(key, value);
        } else {
            MDC.remove(key);
        }
    }

    @Override
    public void finish() {
        // Also nothing here
    }
}
