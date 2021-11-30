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
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;

import javax.annotation.Nonnull;

/**
 * A wrapper for a {@link ThreadBoundRunnable} with idle and execution timings.
 */
final class TimedThreadBoundRunnable<T> implements ThreadBoundRunnable<T> {
    private final int thread;
    private final MeterRegistry registry;
    private final Timer executionTimer;
    private final Timer idleTimer;
    private final ThreadBoundRunnable<T> delegate;
    private final Timer.Sample idleSample;

    static <T> TimedThreadBoundRunnable<T> wrap(
        int thread,
        @Nonnull ThreadBoundRunnable<T> delegate,
        @Nonnull ThreadBoundExecutorMonitor meterConfig)
    {
        if (delegate instanceof TimedThreadBoundRunnable) {
            return (TimedThreadBoundRunnable<T>) delegate;
        }
        return new TimedThreadBoundRunnable<>(
            thread,
            meterConfig.getConfiguration().getRegistry(),
            meterConfig.getExecutionTimerFor(delegate),
            meterConfig.getIdleTimerFor(delegate),
            delegate
        );
    }

    private TimedThreadBoundRunnable(
        int thread,
        MeterRegistry registry,
        Timer executionTimer,
        Timer idleTimer,
        ThreadBoundRunnable<T> delegate)
    {
        this.thread = thread;
        this.registry = registry;
        this.executionTimer = executionTimer;
        this.idleTimer = idleTimer;
        this.delegate = delegate;
        this.idleSample = Timer.start(registry);
    }

    @Override
    public void run() {
        // TODO time delivery times here
        // if the delegate has an internal message, we can get the time of origin from the ID
        idleSample.stop(idleTimer);
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
