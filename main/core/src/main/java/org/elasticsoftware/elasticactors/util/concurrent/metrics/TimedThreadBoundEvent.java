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
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundEvent;

import javax.annotation.Nonnull;

/**
 * A wrapper for a {@link ThreadBoundEvent} with idle and execution timings.
 */
final class TimedThreadBoundEvent<T> implements ThreadBoundEvent<T> {
    private final MeterRegistry registry;
    private final Timer executionTimer;
    private final Timer idleTimer;
    private final ThreadBoundEvent<T> delegate;
    private final Timer.Sample idleSample;
    private Timer.Sample executionSample;

    static <T> TimedThreadBoundEvent<T> wrap(
        @Nonnull ThreadBoundEvent<T> delegate,
        @Nonnull MeterConfiguration meterConfiguration)
    {
        if (delegate instanceof TimedThreadBoundEvent) {
            return (TimedThreadBoundEvent<T>) delegate;
        }
        return new TimedThreadBoundEvent<>(
            meterConfiguration.getRegistry(),
            meterConfiguration.getExecutionTimer(),
            meterConfiguration.getIdleTimer(),
            delegate
        );
    }

    private TimedThreadBoundEvent(
        MeterRegistry registry,
        Timer executionTimer,
        Timer idleTimer,
        ThreadBoundEvent<T> delegate)
    {
        this.registry = registry;
        this.executionTimer = executionTimer;
        this.idleTimer = idleTimer;
        this.delegate = delegate;
        this.idleSample = Timer.start(registry);
    }

    public void reportStart() {
        idleSample.stop(idleTimer);
        executionSample = Timer.start(registry);
    }

    public ThreadBoundEvent<T> getDelegate() {
        return delegate;
    }

    public void reportEnd() {
        executionSample.stop(executionTimer);
    }

    @Override
    public T getKey() {
        return delegate.getKey();
    }
}
