/*
 * Copyright 2013 - 2024 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEventProcessor;

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
