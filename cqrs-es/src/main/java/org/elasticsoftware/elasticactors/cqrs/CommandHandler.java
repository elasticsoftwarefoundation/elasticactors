package org.elasticsoftware.elasticactors.cqrs;

import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorSystem;

@FunctionalInterface
public interface CommandHandler<R extends CommandResponse,C extends Command,S extends ReadOnlyState> {
    R handle(C command, S state, ActorSystem actorSystem);
}
