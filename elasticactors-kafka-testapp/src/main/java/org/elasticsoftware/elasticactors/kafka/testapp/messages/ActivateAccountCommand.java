package org.elasticsoftware.elasticactors.kafka.testapp.messages;

import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Message;

@Message(serializationFramework = JacksonSerializationFramework.class, immutable = true)
public class ActivateAccountCommand {
}
