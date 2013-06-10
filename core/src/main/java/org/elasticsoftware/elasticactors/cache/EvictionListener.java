package org.elasticsoftware.elasticactors.cache;

/**
 * @author Joost van de Wijgerd
 */
public interface EvictionListener<V> {
    void onEvicted(V value);
}
