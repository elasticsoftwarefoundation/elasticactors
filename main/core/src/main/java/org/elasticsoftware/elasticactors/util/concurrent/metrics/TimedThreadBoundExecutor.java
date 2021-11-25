package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEvent;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

public abstract class TimedThreadBoundExecutor implements ThreadBoundExecutor {

    private final ThreadBoundEventProcessor eventProcessor;
    private final MeterConfiguration meterConfiguration;

    protected TimedThreadBoundExecutor(
        @Nonnull ThreadBoundEventProcessor eventProcessor,
        @Nullable MeterConfiguration meterConfiguration)
    {
        this.eventProcessor = eventProcessor;
        this.meterConfiguration = meterConfiguration;
    }

    private boolean isMeterEnabled() {
        return meterConfiguration != null;
    }

    @Nonnull
    protected ThreadBoundEvent prepare(@Nonnull ThreadBoundEvent event)
    {
        if (event instanceof ThreadBoundRunnable) {
            return wrapRunnable((ThreadBoundRunnable) event);
        } else {
            return wrapEvent(event);
        }
    }

    protected void processBatch(@Nonnull List<ThreadBoundEvent> batch) {
        reportStart(batch);
        try {
            eventProcessor.process(unwrap(batch));
        } finally {
            reportEnd(batch);
        }
    }

    protected void processEvent(@Nonnull ThreadBoundEvent event) {
        reportStart(event);
        try {
            eventProcessor.process(unwrap(event));
        } finally {
            reportEnd(event);
        }
    }

    private void reportStart(@Nonnull ThreadBoundEvent event) {
        if (isMeterEnabled() && event instanceof TimedThreadBoundEvent) {
            ((TimedThreadBoundEvent<?>) event).reportStart();
        }
    }

    private void reportStart(@Nonnull List<ThreadBoundEvent> events) {
        if (isMeterEnabled()) {
            for (ThreadBoundEvent event : events) {
                reportStart(event);
            }
        }
    }

    private void reportEnd(@Nonnull ThreadBoundEvent event) {
        if (isMeterEnabled() && event instanceof TimedThreadBoundEvent) {
            ((TimedThreadBoundEvent<?>) event).reportEnd();
        }
    }

    private void reportEnd(@Nonnull List<ThreadBoundEvent> events) {
        if (isMeterEnabled()) {
            for (ThreadBoundEvent event : events) {
                reportEnd(event);
            }
        }
    }

    @Nonnull
    private ThreadBoundEvent wrapRunnable(@Nonnull ThreadBoundRunnable event)
    {
        if (getManager().isTracingEnabled()) {
            event = TraceThreadBoundRunnable.wrap(event);
        }
        if (isMeterEnabled()) {
            // Wrapping metrics must always be the last one
            event = TimedThreadBoundRunnable.wrap(event, meterConfiguration);
        }
        return event;
    }

    @Nonnull
    private ThreadBoundEvent wrapEvent(@Nonnull ThreadBoundEvent event) {
        if (isMeterEnabled()) {
            event = TimedThreadBoundEvent.wrap(event, meterConfiguration);
        }
        return event;
    }

    private List<ThreadBoundEvent> unwrap(@Nonnull List<ThreadBoundEvent> events) {
        if (needsUnwrapping(events)) {
            List<ThreadBoundEvent> unwrapped = new ArrayList<>(events.size());
            for (ThreadBoundEvent e : events) {
                unwrapped.add(unwrap(e));
            }
            return unwrapped;
        } else {
            return events;
        }
    }

    private boolean needsUnwrapping(@Nonnull List<ThreadBoundEvent> events) {
        if (isMeterEnabled()) {
            for (ThreadBoundEvent e : events) {
                if (e instanceof TimedThreadBoundEvent) {
                    return true;
                }
            }
        }
        return false;
    }

    @Nonnull
    private ThreadBoundEvent unwrap(@Nonnull ThreadBoundEvent event) {
        if (isMeterEnabled() && event instanceof TimedThreadBoundEvent) {
            return ((TimedThreadBoundEvent<?>) event).getDelegate();
        }
        return event;
    }
}
