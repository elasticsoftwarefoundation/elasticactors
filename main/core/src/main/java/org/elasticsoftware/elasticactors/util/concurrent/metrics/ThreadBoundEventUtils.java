package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEvent;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.springframework.lang.Nullable;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

public final class ThreadBoundEventUtils {

    private ThreadBoundEventUtils() {
    }

    public static void reportStart(@Nonnull ThreadBoundEvent event) {
        if (event instanceof TimedThreadBoundEvent) {
            ((TimedThreadBoundEvent<?>) event).reportStart();
        }
    }

    public static void reportStart(@Nonnull List<ThreadBoundEvent> events) {
        events.forEach(ThreadBoundEventUtils::reportStart);
    }

    public static void reportEnd(@Nonnull ThreadBoundEvent event) {
        if (event instanceof TimedThreadBoundEvent) {
            ((TimedThreadBoundEvent<?>) event).reportEnd();
        }
    }

    public static void reportEnd(@Nonnull List<ThreadBoundEvent> events) {
        events.forEach(ThreadBoundEventUtils::reportEnd);
    }

    @Nonnull
    public static ThreadBoundEvent prepare(
        @Nonnull ThreadBoundEvent event,
        @Nullable MeterConfiguration meterConfig)
    {
        if (event instanceof ThreadBoundRunnable) {
            return wrapRunnable((ThreadBoundRunnable) event, meterConfig);
        } else {
            return wrapEvent(event, meterConfig);
        }
    }

    @Nonnull
    private static ThreadBoundEvent wrapRunnable(
        @Nonnull ThreadBoundRunnable event,
        @Nullable MeterConfiguration meterConfig)
    {
        if (getManager().isTracingEnabled()) {
            event = TraceThreadBoundRunnable.wrap(event);
        }
        if (meterConfig != null) {
            // Wrapping metrics must always be the last one
            event = TimedThreadBoundRunnable.wrap(event, meterConfig);
        }
        return event;
    }

    @Nonnull
    private static ThreadBoundEvent wrapEvent(
        @Nonnull ThreadBoundEvent event,
        @Nullable MeterConfiguration meterConfig)
    {
        if (meterConfig != null) {
            event = TimedThreadBoundEvent.wrap(event, meterConfig);
        }
        return event;
    }

    public static void processBatch(
        @Nonnull ThreadBoundEventProcessor eventProcessor,
        @Nonnull List<ThreadBoundEvent> batch)
    {
        ThreadBoundEventUtils.reportStart(batch);
        try {
            eventProcessor.process(unwrap(batch));
        } finally {
            ThreadBoundEventUtils.reportEnd(batch);
        }
    }

    private static List<ThreadBoundEvent> unwrap(@Nonnull List<ThreadBoundEvent> events) {
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

    private static boolean needsUnwrapping(@Nonnull List<ThreadBoundEvent> events) {
        for (ThreadBoundEvent e : events) {
            if (e instanceof TimedThreadBoundEvent) {
                return true;
            }
        }
        return false;
    }

    @Nonnull
    public static ThreadBoundEvent unwrap(@Nonnull ThreadBoundEvent event) {
        if (event instanceof TimedThreadBoundEvent) {
            return ((TimedThreadBoundEvent<?>) event).getDelegate();
        }
        return event;
    }

    public static void processEvent(
        @Nonnull ThreadBoundEventProcessor eventProcessor,
        @Nonnull ThreadBoundEvent event)
    {
        ThreadBoundEventUtils.reportStart(event);
        try {
            eventProcessor.process(unwrap(event));
        } finally {
            ThreadBoundEventUtils.reportEnd(event);
        }
    }
}
