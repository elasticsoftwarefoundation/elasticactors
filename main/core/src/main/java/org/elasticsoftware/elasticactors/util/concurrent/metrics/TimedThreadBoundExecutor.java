package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.BaseUnits;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEvent;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.LongAdder;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;

import static java.lang.String.format;

public abstract class TimedThreadBoundExecutor implements ThreadBoundExecutor {

    private final ThreadBoundEventProcessor eventProcessor;
    private final ThreadBoundExecutorMonitor monitor;

    private Counter[] completedEvents;
    private LongAdder globalQueuedEvents;
    private Counter globalFinishedEvents;
    private LongAdder activeThreadsCount;

    protected TimedThreadBoundExecutor(
        @Nonnull ThreadBoundEventProcessor eventProcessor,
        @Nullable ThreadBoundExecutorMonitor monitor)
    {
        this.eventProcessor = eventProcessor;
        this.monitor = monitor;
    }

    @Override
    public synchronized void init() {
        if (isMeterEnabled()) {

            getLogger().info(
                "Initializing metrics for {} {}",
                getClass().getSimpleName(),
                monitor.getTags()
            );

            int threadCount = getThreadCount();
            
            this.completedEvents = new Counter[threadCount];
            this.globalQueuedEvents = new LongAdder();
            this.activeThreadsCount = new LongAdder();

            Tags tags = monitor.getTags();
            MeterRegistry registry = monitor.getConfiguration().getRegistry();

            Gauge.builder(
                    monitor.createNameForSuffix("threads"),
                    this,
                    ThreadBoundExecutor::getThreadCount
                )
                .tags(tags)
                .description("The number of threads in the ThreadBoundExecutor")
                .baseUnit(BaseUnits.THREADS)
                .register(registry);

            this.globalFinishedEvents =
                Counter.builder(monitor.createNameForSuffix("completed"))
                    .tags(tags)
                    .description(
                        "The approximate total number of tasks that have completed processing")
                    .baseUnit(BaseUnits.TASKS)
                    .register(registry);

            Gauge.builder(
                    monitor.createNameForSuffix("active"),
                    activeThreadsCount,
                    LongAdder::sum
                )
                .tags(tags)
                .description("The approximate number of threads that are actively processing tasks")
                .baseUnit(BaseUnits.THREADS)
                .register(registry);

            Gauge.builder(
                    monitor.createNameForSuffix("queued"),
                    globalQueuedEvents,
                    LongAdder::sum
                )
                .tags(tags)
                .description("The approximate number of tasks that are queued for execution")
                .baseUnit(BaseUnits.TASKS)
                .register(registry);

            String structureName = getExecutorDataStructureName();

            for (int i = 0; i < threadCount; i++) {
                Gauge.builder(monitor.createNameForSuffix(format(
                        "%s.remaining.%d",
                        structureName,
                        i
                    )), i, this::getCapacityForThread)
                    .tags(tags)
                    .description(format(
                        "The number of additional tasks the %s for thread %d can ideally accept "
                            + "without blocking",
                        structureName,
                        i
                    ))
                    .baseUnit(BaseUnits.TASKS)
                    .register(registry);

                FunctionCounter.builder(monitor.createNameForSuffix(format(
                        "%s.queued.%d",
                        structureName,
                        i
                    )), i, this::getQueuedEventsForThread)
                    .tags(tags)
                    .description(format(
                        "The number of tasks that that are queued for execution on the %s for "
                            + "thread %d",
                        structureName,
                        i
                    ))
                    .baseUnit(BaseUnits.TASKS)
                    .register(registry);

                this.completedEvents[i] =
                    Counter.builder(monitor.createNameForSuffix(format(
                            "%s.completed.%d",
                            structureName,
                            i
                        )))
                        .tags(tags)
                        .description(format(
                            "The number of tasks that have completed processing on thread %d",
                            i
                        ))
                        .baseUnit(BaseUnits.TASKS)
                        .register(registry);
            }
        }
    }

    protected abstract Logger getLogger();

    @Nonnull
    protected abstract String getExecutorDataStructureName();

    protected abstract long getCapacityForThread(int thread);

    protected abstract long getQueuedEventsForThread(int thread);

    protected abstract boolean isShuttingDown();

    protected abstract void timedExecute(@Nonnull final ThreadBoundEvent event, final int thread);

    protected abstract void incrementQueuedEvents(int thread);

    protected abstract void decrementQueuedEvents(int thread);

    protected final boolean isMeterEnabled() {
        return monitor != null;
    }

    private void reportQueuedItem(int thread) {
        if (isMeterEnabled()) {
            if (getLogger().isTraceEnabled()) {
                getLogger().trace(
                    "Increasing total number of queued events. "
                        + "Thread {}. "
                        + "Total before this increase: {}",
                    thread,
                    globalQueuedEvents.sum()
                );
            }
            globalQueuedEvents.increment();
            incrementQueuedEvents(thread);
        }
    }

    private void reportTakenItem(int thread) {
        if (isMeterEnabled()) {
            if (getLogger().isTraceEnabled()) {
                getLogger().trace(
                    "Decreasing total number of queued events. "
                        + "Thread {}. "
                        + "Total before this increase: {}",
                    thread,
                    globalQueuedEvents.sum()
                );
            }
            globalQueuedEvents.decrement();
            decrementQueuedEvents(thread);
        }
    }

    private void reportThreadStarted(int thread) {
        if (getLogger().isTraceEnabled()) {
            getLogger().trace(
                "Reporting thread {} started processing an event. "
                    + "Current number of active threads before this: {}",
                thread,
                activeThreadsCount.sum()
            );
        }
        activeThreadsCount.increment();
        // An item gets taken from the queue when it starts processing
        reportTakenItem(thread);
    }

    private void reportThreadFinished(int thread) {
        if (getLogger().isTraceEnabled()) {
            getLogger().trace(
                "Reporting thread {} finished processing an event. "
                    + "Current number of active threads before this: {}. "
                    + "Completed tasks for this thread before this: {}. "
                    + "Total number of completed tasks before this: {}.",
                thread,
                activeThreadsCount.sum(),
                completedEvents[thread].count(),
                globalFinishedEvents.count()
            );
        }
        activeThreadsCount.decrement();
        completedEvents[thread].increment();
        globalFinishedEvents.increment();
    }

    @Override
    public final void execute(final ThreadBoundEvent event) {
        if (isShuttingDown()) {
            throw new RejectedExecutionException("The system is shutting down.");
        }
        int thread = getBucket(event);
        reportQueuedItem(thread);
        try {
            timedExecute(prepare(thread, event), thread);
        } catch (Exception e) {
            reportTakenItem(thread);
            throw e;
        }
    }

    private int getBucket(@Nonnull final ThreadBoundEvent event) {
        return Math.abs(event.getKey().hashCode()) % getThreadCount();
    }

    @Nonnull
    private ThreadBoundEvent prepare(int thread, @Nonnull ThreadBoundEvent event)
    {
        if (event instanceof ThreadBoundRunnable) {
            return wrapRunnable(thread, (ThreadBoundRunnable) event);
        } else {
            return wrapEvent(thread, event);
        }
    }

    protected final void processBatch(@Nonnull List<ThreadBoundEvent> batch) {
        reportStart(batch);
        try {
            eventProcessor.process(unwrap(batch));
        } finally {
            reportEnd(batch);
        }
    }

    protected final void processEvent(@Nonnull ThreadBoundEvent event) {
        reportStart(event);
        try {
            eventProcessor.process(unwrap(event));
        } finally {
            reportEnd(event);
        }
    }

    private void reportStart(@Nonnull List<ThreadBoundEvent> events) {
        if (isMeterEnabled()) {
            for (ThreadBoundEvent event : events) {
                reportStart(event);
            }
        }
    }

    private void reportStart(@Nonnull ThreadBoundEvent event) {
        if (isMeterEnabled()) {
            if (event instanceof TimedThreadBoundEvent) {
                TimedThreadBoundEvent<?> timedEvent = (TimedThreadBoundEvent<?>) event;
                timedEvent.reportStart();
                reportThreadStarted(timedEvent.getThread());
            } else if (event instanceof TimedThreadBoundRunnable) {
                TimedThreadBoundRunnable timedRunnable = (TimedThreadBoundRunnable) event;
                reportThreadStarted(timedRunnable.getThread());
            }
        }
    }

    private void reportEnd(@Nonnull List<ThreadBoundEvent> events) {
        if (isMeterEnabled()) {
            for (ThreadBoundEvent event : events) {
                reportEnd(event);
            }
        }
    }

    private void reportEnd(@Nonnull ThreadBoundEvent event) {
        if (isMeterEnabled()) {
            if (event instanceof TimedThreadBoundEvent) {
                TimedThreadBoundEvent<?> timedThreadBoundEvent = (TimedThreadBoundEvent<?>) event;
                timedThreadBoundEvent.reportEnd();
                reportThreadFinished(timedThreadBoundEvent.getThread());
            } else if (event instanceof TimedThreadBoundRunnable) {
                TimedThreadBoundRunnable timedRunnable = (TimedThreadBoundRunnable) event;
                reportThreadFinished(timedRunnable.getThread());
            }
        }
    }

    @Nonnull
    private ThreadBoundEvent wrapRunnable(int thread, @Nonnull ThreadBoundRunnable event)
    {
        if (getManager().isTracingEnabled()) {
            event = TraceThreadBoundRunnable.wrap(event);
        }
        if (isMeterEnabled()) {
            // Wrapping metrics must always be the last one
            event = TimedThreadBoundRunnable.wrap(thread, event, monitor);
        }
        return event;
    }

    @Nonnull
    private ThreadBoundEvent wrapEvent(int thread, @Nonnull ThreadBoundEvent event) {
        if (isMeterEnabled()) {
            event = TimedThreadBoundEvent.wrap(thread, event, monitor);
        }
        return event;
    }

    private List<ThreadBoundEvent> unwrap(@Nonnull List<ThreadBoundEvent> events) {
        if (needsUnwrapping(events)) {
            if (events.size() == 1) {
                return Collections.singletonList(unwrap(events.get(0)));
            } else {
                List<ThreadBoundEvent> unwrapped = new ArrayList<>(events.size());
                for (ThreadBoundEvent e : events) {
                    unwrapped.add(unwrap(e));
                }
                return unwrapped;
            }
        } else {
            return events;
        }
    }

    private boolean needsUnwrapping(@Nonnull List<ThreadBoundEvent> events) {
        if (isMeterEnabled()) {
            if (events.size() == 1) {
                return events.get(0) instanceof TimedThreadBoundEvent;
            } else {
                for (ThreadBoundEvent e : events) {
                    if (e instanceof TimedThreadBoundEvent) {
                        return true;
                    }
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
