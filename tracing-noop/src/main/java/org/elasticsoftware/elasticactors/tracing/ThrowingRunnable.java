package org.elasticsoftware.elasticactors.tracing;

@FunctionalInterface
public interface ThrowingRunnable {

    void run() throws Exception;

}
