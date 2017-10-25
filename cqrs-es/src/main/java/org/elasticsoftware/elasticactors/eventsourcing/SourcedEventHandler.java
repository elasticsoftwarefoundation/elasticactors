package org.elasticsoftware.elasticactors.eventsourcing;

import org.elasticsoftware.elasticactors.ActorState;

@FunctionalInterface
public interface SourcedEventHandler<E extends SourcedEvent, S extends ActorState> {
    void update(E event, S state);
}
