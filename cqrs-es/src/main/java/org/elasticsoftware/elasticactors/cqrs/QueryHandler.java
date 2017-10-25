package org.elasticsoftware.elasticactors.cqrs;

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorSystem;

@FunctionalInterface
public interface QueryHandler<R extends QueryResponse, Q extends Query, S extends ReadOnlyState> {
    R query(Q query, S state, ActorSystem actorSystem);
}
