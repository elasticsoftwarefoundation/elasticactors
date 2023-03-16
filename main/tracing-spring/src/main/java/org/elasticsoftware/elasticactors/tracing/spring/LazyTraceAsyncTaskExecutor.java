/*
 * Copyright 2013 - 2023 The Original Authors
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
