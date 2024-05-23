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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TraceableExecutorService implements ExecutorService {

    protected final ExecutorService delegate;

    public TraceableExecutorService(final ExecutorService delegate) {
        this.delegate = delegate;
    }

    @Override
    public void execute(Runnable command) {
        this.delegate.execute(TraceRunnable.wrap(command));
    }

    @Override
    public void shutdown() {
        this.delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return this.delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return this.delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return this.delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        return this.delegate.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return this.delegate.submit(TraceCallable.wrap(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return this.delegate.submit(TraceRunnable.wrap(task), result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return this.delegate.submit(TraceRunnable.wrap(task));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        return this.delegate.invokeAll(wrapCallableCollection(tasks));
    }

    @Override
    public <T> List<Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks,
            long timeout, TimeUnit unit) throws InterruptedException {
        return this.delegate.invokeAll(wrapCallableCollection(tasks), timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
        return this.delegate.invokeAny(wrapCallableCollection(tasks));
    }

    @Override
    public <T> T invokeAny(
            Collection<? extends Callable<T>> tasks, long timeout,
            TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return this.delegate.invokeAny(wrapCallableCollection(tasks), timeout, unit);
    }

    private <T> Collection<? extends Callable<T>> wrapCallableCollection(
            Collection<? extends Callable<T>> tasks) {
        List<Callable<T>> ts = new ArrayList<>();
        for (Callable<T> task : tasks) {
            if (!(task instanceof TraceCallable)) {
                ts.add(TraceCallable.wrap(task));
            }
        }
        return ts;
    }

}
