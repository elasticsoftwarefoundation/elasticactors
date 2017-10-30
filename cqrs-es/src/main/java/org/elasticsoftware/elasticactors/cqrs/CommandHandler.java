package org.elasticsoftware.elasticactors.cqrs;

import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.eventsourcing.SourcedEvent;

import java.util.function.Consumer;
import java.util.function.Supplier;

@FunctionalInterface
public interface CommandHandler<C extends Command,S extends ReadOnlyState> {
    Supplier<CommandResponse> handle(C command, S state, Consumer<SourcedEvent> eventConsumer, ActorSystem actorSystem);
}
