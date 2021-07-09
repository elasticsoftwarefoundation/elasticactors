package org.elasticsoftware.elasticactors.tracing.configuration;

import java.util.concurrent.Executor;

class LazyTraceExecutor implements Executor {

	private final Executor delegate;

	LazyTraceExecutor(Executor delegate) {
		this.delegate = delegate;
	}

	@Override
	public void execute(Runnable command) {
		this.delegate.execute(TraceRunnable.wrap(command));
	}

}
