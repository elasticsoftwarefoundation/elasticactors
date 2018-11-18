package org.elasticsoftware.elasticactors.client.common;

import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;

public final class ThreadBoundRunnableWrapper implements ThreadBoundRunnable<String> {
    private final String key;
    private final Runnable delegate;

    public ThreadBoundRunnableWrapper(String key, Runnable delegate) {
        this.key = key;
        this.delegate = delegate;
    }

    @Override
    public void run() {
        delegate.run();
    }

    @Override
    public String getKey() {
        return key;
    }
}
