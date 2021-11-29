package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class DelegatingTimedThreadBoundExecutor extends TimedThreadBoundExecutor {

    protected DelegatingTimedThreadBoundExecutor(
        @Nonnull ThreadBoundEventProcessor eventProcessor,
        @Nullable ThreadBoundExecutorMonitor monitor)
    {
        super(eventProcessor, monitor);
    }

    @Override
    protected final void incrementQueuedEvents(int thread) {
        // Nothing to do. Let the internal data structure count that.
    }

    @Override
    protected final void decrementQueuedEvents(int thread) {
        // Nothing to do. Let the internal data structure count that.
    }
}
