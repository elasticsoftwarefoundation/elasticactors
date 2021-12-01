/**
 * Copyright 2019 VMware, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsoftware.elasticactors.util.concurrent.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.util.concurrent.MessageHandlingThreadBoundRunnable;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.elasticsoftware.elasticactors.util.concurrent.metrics.ThreadBoundExecutorMonitor.TimerType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper for a {@link ThreadBoundRunnable} with idle and execution timings.
 */
final class TimedThreadBoundRunnable<T> implements ThreadBoundRunnable<T> {
    private final int thread;
    private final MeterRegistry registry;
    private final Timer executionTimer;
    private final Timer idleTimer;
    private final Timer deliveryTimer;
    private final ThreadBoundRunnable<T> delegate;
    private final Timer.Sample idleSample;

    static <T> TimedThreadBoundRunnable<T> wrap(
        int thread,
        @Nonnull ThreadBoundRunnable<T> delegate,
        @Nonnull ThreadBoundExecutorMonitor monitor)
    {
        if (delegate instanceof TimedThreadBoundRunnable) {
            return (TimedThreadBoundRunnable<T>) delegate;
        }
        Map<TimerType, Timer> timers = monitor.getTimersFor(delegate);
        return new TimedThreadBoundRunnable<>(
            thread,
            monitor.getConfiguration().getRegistry(),
            timers.get(TimerType.EXECUTION),
            timers.get(TimerType.IDLE),
            timers.get(TimerType.DELIVERY),
            delegate
        );
    }

    private TimedThreadBoundRunnable(
        int thread,
        MeterRegistry registry,
        Timer executionTimer,
        Timer idleTimer,
        @Nullable Timer deliveryTimer,
        ThreadBoundRunnable<T> delegate)
    {
        this.thread = thread;
        this.registry = registry;
        this.executionTimer = executionTimer;
        this.idleTimer = idleTimer;
        this.delegate = delegate;
        this.deliveryTimer = deliveryTimer;
        this.idleSample = Timer.start(registry);
    }

    @Override
    public void run() {
        idleSample.stop(idleTimer);
        if (deliveryTimer != null && delegate instanceof MessageHandlingThreadBoundRunnable) {
            MessageHandlingThreadBoundRunnable<?> mhtbRunnable =
                (MessageHandlingThreadBoundRunnable<?>) delegate;
            long timestamp = UUIDTools.toUnixTimestamp(mhtbRunnable.getInternalMessage().getId());
            long delay = (System.currentTimeMillis() - timestamp);
            deliveryTimer.record(delay, TimeUnit.MILLISECONDS);
        }
        Timer.Sample executionSample = Timer.start(registry);
        try {
            delegate.run();
        } finally {
            executionSample.stop(executionTimer);
        }
    }

    @Override
    public T getKey() {
        return delegate.getKey();
    }

    public int getThread() {
        return thread;
    }
}
