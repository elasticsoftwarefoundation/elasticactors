package org.elasticsoftware.elasticactors.tracing;

import javax.annotation.Nullable;

public interface Traceable {

    @Nullable
    TraceContext getTraceContext();

    @Nullable
    CreationContext getCreationContext();
}
