package org.elasticsoftware.elasticactors.eventsourcing.commands;

import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.cqrs.Command;
import org.elasticsoftware.elasticactors.serialization.Message;

@Message(serializationFramework = JacksonSerializationFramework.class, immutable = true, durable = true)
public class SettleTransferCommand implements Command {
}
