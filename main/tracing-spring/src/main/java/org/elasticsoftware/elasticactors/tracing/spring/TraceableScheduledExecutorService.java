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

package org.elasticsoftware.elasticactors.tracing.spring;

import java.util.concurrent.*;

public class TraceableScheduledExecutorService extends TraceableExecutorService
        implements ScheduledExecutorService {

    public TraceableScheduledExecutorService(final ExecutorService delegate) {
        super(delegate);
    }

    private ScheduledExecutorService getScheduledExecutorService() {
        return (ScheduledExecutorService) this.delegate;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return getScheduledExecutorService().schedule(TraceRunnable.wrap(command), delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(
            Callable<V> callable, long delay,
            TimeUnit unit) {
        return getScheduledExecutorService().schedule(TraceCallable.wrap(callable), delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command, long initialDelay,
            long period, TimeUnit unit) {
        return getScheduledExecutorService().scheduleAtFixedRate(
                TraceRunnable.wrap(command),
                initialDelay,
                period,
                unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command, long initialDelay,
            long delay, TimeUnit unit) {
        return getScheduledExecutorService().scheduleWithFixedDelay(
                TraceRunnable.wrap(command),
                initialDelay,
                delay,
                unit);
    }

}
