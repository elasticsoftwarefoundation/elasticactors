package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.atomic.LongAdder;

public abstract class CountingTimedThreadBoundExecutor extends TimedThreadBoundExecutor {

    private LongAdder[] queuedEvents;

    protected CountingTimedThreadBoundExecutor(
        @Nonnull ThreadBoundEventProcessor eventProcessor,
        @Nullable ThreadBoundExecutorMonitor monitor)
    {
        super(eventProcessor, monitor);
    }

    @Override
    public synchronized void init() {
        if (isMeterEnabled()) {
            int threadCount = getThreadCount();
            this.queuedEvents = new LongAdder[threadCount];
            for (int i = 0; i < threadCount; i++) {
                this.queuedEvents[i] = new LongAdder();
            }
        }
        // This must be initialized after the counters here
        super.init();
    }

    @Override
    protected final void incrementQueuedEvents(int thread, int itemCount) {
        queuedEvents[thread].add(itemCount);
    }

    @Override
    protected final void decrementQueuedEvents(int thread, int itemCount) {
        queuedEvents[thread].add(-itemCount);
    }

    @Override
    protected final long getCapacityForThread(int thread)  {
        return getCapacityForThread(thread, queuedEvents[thread].sum());
    }

    @Override
    protected final long getQueuedEventsForThread(int thread) {
        return getQueuedEventsForThread(thread, queuedEvents[thread].sum());
    }

    protected abstract long getCapacityForThread(int thread, long currentCount);

    protected abstract long getQueuedEventsForThread(int thread, long currentCount);
}
