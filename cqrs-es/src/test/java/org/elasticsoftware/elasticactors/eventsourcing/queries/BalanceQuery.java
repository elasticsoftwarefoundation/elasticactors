package org.elasticsoftware.elasticactors.eventsourcing.queries;

import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.cqrs.api.Query;
import org.elasticsoftware.elasticactors.serialization.Message;

@Message(serializationFramework = JacksonSerializationFramework.class, immutable = true, durable = false)
public class BalanceQuery implements Query {
}
