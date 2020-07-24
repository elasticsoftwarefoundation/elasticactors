package org.elasticsoftware.elasticactors.tracing.service;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface DiagnosticContext {

    void init();

    void put(@Nonnull String key, @Nullable String value);

    void finish();

}
