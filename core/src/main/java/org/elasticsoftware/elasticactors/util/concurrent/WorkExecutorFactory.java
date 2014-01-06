package org.elasticsoftware.elasticactors.util.concurrent;

/**
 * @author Joost van de Wijgerd
 */
public interface WorkExecutorFactory<T extends WorkExecutor> {
    T create();
}
