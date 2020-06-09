package org.elasticsoftware.elasticactors.tracing.configuration;

import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.AsyncConfigurerSupport;

import java.util.concurrent.Executor;

public class LazyTraceAsyncCustomizer extends AsyncConfigurerSupport {

	private final AsyncConfigurer delegate;

	public LazyTraceAsyncCustomizer(AsyncConfigurer delegate) {
		this.delegate = delegate;
	}

	@Override
	public Executor getAsyncExecutor() {
		if (this.delegate.getAsyncExecutor() instanceof LazyTraceExecutor) {
			return this.delegate.getAsyncExecutor();
		}
		return new LazyTraceExecutor(this.delegate.getAsyncExecutor());
	}

	@Override
	public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
		return this.delegate.getAsyncUncaughtExceptionHandler();
	}

}
