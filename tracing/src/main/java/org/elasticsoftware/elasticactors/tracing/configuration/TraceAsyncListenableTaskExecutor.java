package org.elasticsoftware.elasticactors.tracing.configuration;

import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class TraceAsyncListenableTaskExecutor implements AsyncListenableTaskExecutor {

    private final AsyncListenableTaskExecutor delegate;

    TraceAsyncListenableTaskExecutor(AsyncListenableTaskExecutor delegate) {
        this.delegate = delegate;
    }

    @Override
    public ListenableFuture<?> submitListenable(Runnable task) {
        return this.delegate.submitListenable(TraceRunnable.wrap(task));
    }

    @Override
    public <T> ListenableFuture<T> submitListenable(Callable<T> task) {
        return this.delegate.submitListenable(TraceCallable.wrap(task));
    }

    @Override
    public void execute(Runnable task, long startTimeout) {
        this.delegate.execute(TraceRunnable.wrap(task), startTimeout);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return this.delegate.submit(TraceRunnable.wrap(task));
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return this.delegate.submit(TraceCallable.wrap(task));
    }

    @Override
    public void execute(Runnable task) {
        this.delegate.execute(TraceRunnable.wrap(task));
    }

}
