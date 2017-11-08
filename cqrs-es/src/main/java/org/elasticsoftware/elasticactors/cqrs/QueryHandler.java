package org.elasticsoftware.elasticactors.cqrs;

import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.cqrs.api.Query;
import org.elasticsoftware.elasticactors.cqrs.api.QueryResponse;

@FunctionalInterface
public interface QueryHandler<R extends QueryResponse, Q extends Query, S extends ReadOnlyState> {
    R query(Q query, S state, ActorSystem actorSystem);
}
