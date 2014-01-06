package org.elasticsoftware.elasticactors.util.concurrent;

/**
 * @author Joost van de Wijgerd
 */
public interface WorkExecutor<T> {
    void execute(T work);
}
