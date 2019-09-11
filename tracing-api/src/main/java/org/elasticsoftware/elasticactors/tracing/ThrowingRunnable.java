package org.elasticsoftware.elasticactors.tracing;

@FunctionalInterface
public interface ThrowingRunnable<E extends Throwable> {

    void run() throws E;

}
