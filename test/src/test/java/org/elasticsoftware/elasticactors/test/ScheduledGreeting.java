package org.elasticsoftware.elasticactors.test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;
import org.elasticsoftware.elasticactors.serialization.Message;

/**
 * @author Joost van de Wijgerd
 */
@Message(serializationFramework = JacksonSerializationFramework.class,durable = true)
public final class ScheduledGreeting {
    private final ScheduledMessageRef messageRef;

    @JsonCreator
    public ScheduledGreeting(@JsonProperty("messageRef") ScheduledMessageRef messageRef) {
        this.messageRef = messageRef;
    }

    public ScheduledMessageRef getMessageRef() {
        return messageRef;
    }
}
