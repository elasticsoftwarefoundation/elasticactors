package org.elasticsoftware.elasticactors.tracing.spring;

import org.springframework.core.task.AsyncTaskExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

class LazyTraceAsyncTaskExecutor implements AsyncTaskExecutor {

	private final AsyncTaskExecutor delegate;

	LazyTraceAsyncTaskExecutor(AsyncTaskExecutor delegate) {
		this.delegate = delegate;
	}

	@Override
	public void execute(Runnable task) {
		this.delegate.execute(TraceRunnable.wrap(task));
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

}
