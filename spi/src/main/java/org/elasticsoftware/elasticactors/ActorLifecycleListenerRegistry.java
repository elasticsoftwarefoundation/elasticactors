package org.elasticsoftware.elasticactors;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public interface ActorLifecycleListenerRegistry {
    List<ActorLifecycleListener<?>> getListeners(Class<? extends ElasticActor> actorClass);
}
