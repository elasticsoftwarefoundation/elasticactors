package org.elasticsoftware.elasticactors.tracing.spring;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
