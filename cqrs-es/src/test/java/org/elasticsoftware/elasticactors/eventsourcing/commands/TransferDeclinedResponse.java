package org.elasticsoftware.elasticactors.eventsourcing.commands;

import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.cqrs.api.CommandResponse;
import org.elasticsoftware.elasticactors.serialization.Message;

@Message(serializationFramework = JacksonSerializationFramework.class, immutable = true, durable = false)
public class TransferDeclinedResponse implements CommandResponse {
}
