package org.elasticsoftware.elasticactors.cqrs;

import org.elasticsoftware.elasticactors.eventsourcing.SourcedEvent;

import java.util.LinkedList;
import java.util.List;

public final class CommandContext {
    private final Command command;
    private final List<SourcedEvent> events = new LinkedList<>();

    public CommandContext(Command command) {
        this.command = command;
    }

    public Command getCommand() {
        return command;
    }

    public void addEvent(SourcedEvent event) {
        this.events.add(event);
    }

    public List<SourcedEvent> getEvents() {
        return events;
    }

}
