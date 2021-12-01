package org.elasticsoftware.elasticactors.util.concurrent;

import javax.annotation.Nonnull;

public interface WrapperThreadBoundRunnable<T> extends ThreadBoundRunnable<T> {

    @Nonnull
    ThreadBoundRunnable<T> getWrappedRunnable();

    @Nonnull
    default ThreadBoundRunnable<T> unwrap() {
        ThreadBoundRunnable<T> unwrapped = getWrappedRunnable();
        while (unwrapped instanceof WrapperThreadBoundRunnable) {
            unwrapped = ((WrapperThreadBoundRunnable<T>) unwrapped).getWrappedRunnable();
        }
        return unwrapped;
    }
}
