package org.elasticsoftware.elasticactors.tracing;

@FunctionalInterface
public interface ThrowingSupplier<T, E extends Throwable> {

    T get() throws E;

}
