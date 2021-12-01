package org.elasticsoftware.elasticactors.util.concurrent.metrics;

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

    private LongAdder globalQueuedEvents;
    private LongAdder[] completedEvents;
    private LongAdder globalCompletedEvents;
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

            this.globalQueuedEvents = new LongAdder();
            this.completedEvents = new LongAdder[threadCount];
            this.globalCompletedEvents = new LongAdder();
            this.activeThreadsCount = new LongAdder();

            for (int i = 0; i < threadCount; i++) {
                this.completedEvents[i] = new LongAdder();
            }

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

            FunctionCounter.builder(
                    monitor.createNameForSuffix("completed"),
                    globalCompletedEvents,
                    LongAdder::sum
                )
                .tags(tags)
                .description("The approximate total number of tasks that have completed processing")
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
                int threadName = i + 1;
                Gauge.builder(
                        monitor.createNameForSuffix(format(
                            "%s.remaining.%d",
                            structureName,
                            threadName
                        )),
                        i,
                        this::getCapacityForThread
                    )
                    .tags(tags)
                    .description(format(
                        "The number of additional tasks the %s for thread %d can ideally accept "
                            + "without blocking",
                        structureName,
                        threadName
                    ))
                    .baseUnit(BaseUnits.TASKS)
                    .register(registry);

                Gauge.builder(
                        monitor.createNameForSuffix(format(
                            "%s.queued.%d",
                            structureName,
                            threadName
                        )),
                        i,
                        this::getQueuedEventsForThread
                    )
                    .tags(tags)
                    .description(format(
                        "The number of tasks that that are queued for execution on the %s for "
                            + "thread %d",
                        structureName,
                        threadName
                    ))
                    .baseUnit(BaseUnits.TASKS)
                    .register(registry);

                FunctionCounter.builder(
                        monitor.createNameForSuffix(format(
                            "%s.completed.%d",
                            structureName,
                            threadName
                        )),
                        this.completedEvents[i],
                        LongAdder::sum
                    )
                    .tags(tags)
                    .description(format(
                        "The number of tasks that have completed processing on thread %d",
                        threadName
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

    protected abstract void timedExecute(final int thread, @Nonnull final ThreadBoundEvent event);

    protected abstract void incrementQueuedEvents(int thread, int itemCount);

    protected abstract void decrementQueuedEvents(int thread, int itemCount);

    protected final boolean isMeterEnabled() {
        return monitor != null;
    }

    private void reportQueuedItem(int thread, int itemCount) {
        if (isMeterEnabled()) {
            if (getLogger().isTraceEnabled()) {
                getLogger().trace(
                    "Reporting thread {} queued {} event(s). "
                        + "Total number of queued events before this: {}. "
                        + "Queued events on thread {} before this: {}",
                    thread + 1,
                    itemCount,
                    globalQueuedEvents.sum(),
                    thread + 1,
                    getQueuedEventsForThread(thread)
                );
            }
            globalQueuedEvents.add(itemCount);
            incrementQueuedEvents(thread, itemCount);
        }
    }

    private void reportTakenItem(int thread, int itemCount) {
        if (isMeterEnabled()) {
            if (getLogger().isTraceEnabled()) {
                getLogger().trace(
                    "Reporting thread {} took {} event(s). "
                        + "Total number of queued events before this: {}. "
                        + "Queued events on thread {} before this: {}",
                    thread + 1,
                    itemCount,
                    globalQueuedEvents.sum(),
                    thread + 1,
                    getQueuedEventsForThread(thread)
                );
            }
            globalQueuedEvents.add(-itemCount);
            decrementQueuedEvents(thread, itemCount);
        }
    }

    private void reportThreadStarted(int thread, int itemCount) {
        if (getLogger().isTraceEnabled()) {
            getLogger().trace(
                "Reporting thread {} started processing {} event(s). "
                    + "Current number of active threads before this: {}",
                thread + 1,
                itemCount,
                activeThreadsCount.sum()
            );
        }
        activeThreadsCount.increment();
        // An item gets taken from the queue when it starts processing
        reportTakenItem(thread, itemCount);
    }

    private void reportThreadFinished(int thread, int itemCount) {
        if (getLogger().isTraceEnabled()) {
            getLogger().trace(
                "Reporting thread {} finished processing {} event(s). "
                    + "Current number of active threads before this: {}. "
                    + "Completed tasks for this thread before this: {}. "
                    + "Total number of completed tasks before this: {}.",
                thread + 1,
                itemCount,
                activeThreadsCount.sum(),
                completedEvents[thread].sum(),
                globalCompletedEvents.sum()
            );
        }
        activeThreadsCount.decrement();
        completedEvents[thread].add(itemCount);
        globalCompletedEvents.add(itemCount);
    }

    @Override
    public final void execute(final ThreadBoundEvent event) {
        if (isShuttingDown()) {
            throw new RejectedExecutionException("The system is shutting down.");
        }
        int thread = getThread(event);
        reportQueuedItem(thread, 1);
        try {
            timedExecute(thread, prepare(event));
        } catch (Exception e) {
            reportTakenItem(thread, 1);
            throw e;
        }
    }

    private int getThread(@Nonnull final ThreadBoundEvent event) {
        return Math.abs(event.getKey().hashCode()) % getThreadCount();
    }

    @Nonnull
    private ThreadBoundEvent prepare(@Nonnull ThreadBoundEvent event)
    {
        if (event instanceof ThreadBoundRunnable) {
            return wrapRunnable((ThreadBoundRunnable) event);
        } else {
            return wrapEvent(event);
        }
    }

    protected final void processBatch(int thread, @Nonnull List<ThreadBoundEvent> batch) {
        reportStartBatch(thread, batch);
        try {
            eventProcessor.process(unwrapBatch(batch));
        } finally {
            reportEndBatch(thread, batch);
        }
    }

    protected final void processEvent(int thread, @Nonnull ThreadBoundEvent event) {
        reportStart(thread, event);
        try {
            eventProcessor.process(unwrap(event));
        } finally {
            reportEnd(thread, event);
        }
    }

    private void reportStartBatch(int thread, @Nonnull List<ThreadBoundEvent> events) {
        if (isMeterEnabled()) {
            reportThreadStarted(thread, events.size());
            for (ThreadBoundEvent event : events) {
                reportStart(thread, event, true);
            }
        }
    }

    private void reportStart(int thread, @Nonnull ThreadBoundEvent event) {
        reportStart(thread, event, false);
    }

    private void reportStart(int thread, @Nonnull ThreadBoundEvent event, boolean isBatch) {
        if (isMeterEnabled()) {
            if (event instanceof TimedThreadBoundEvent) {
                ((TimedThreadBoundEvent<?>) event).reportStart();
            }
            if (!isBatch) {
                reportThreadStarted(thread, 1);
            }
        }
    }

    private void reportEndBatch(int thread, @Nonnull List<ThreadBoundEvent> events) {
        if (isMeterEnabled()) {
            reportThreadFinished(thread, events.size());
            for (ThreadBoundEvent event : events) {
                reportEnd(thread, event, true);
            }
        }
    }

    private void reportEnd(int thread, @Nonnull ThreadBoundEvent event) {
        reportEnd(thread, event, false);
    }

    private void reportEnd(int thread, @Nonnull ThreadBoundEvent event, boolean isBatch) {
        if (isMeterEnabled()) {
            if (event instanceof TimedThreadBoundEvent) {
                ((TimedThreadBoundEvent<?>) event).reportEnd();
            }
            if (!isBatch) {
                reportThreadFinished(thread, 1);
            }
        }
    }

    @Nonnull
    private ThreadBoundRunnable wrapRunnable(@Nonnull ThreadBoundRunnable runnable)
    {
        if (getManager().isTracingEnabled()) {
            runnable = TraceThreadBoundRunnable.wrap(runnable);
        }
        if (isMeterEnabled()) {
            // Wrapping metrics must always be the last one
            runnable = TimedThreadBoundRunnable.wrap(runnable, monitor);
        }
        return runnable;
    }

    @Nonnull
    private ThreadBoundEvent wrapEvent(@Nonnull ThreadBoundEvent event) {
        if (isMeterEnabled()) {
            event = TimedThreadBoundEvent.wrap(event, monitor);
        }
        return event;
    }

    private List<ThreadBoundEvent> unwrapBatch(@Nonnull List<ThreadBoundEvent> events) {
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
