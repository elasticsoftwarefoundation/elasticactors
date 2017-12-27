package org.elasticsoftware.elasticactors.kafka.testapp.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Message;

@Message(serializationFramework = JacksonSerializationFramework.class, immutable = true)
public class ScheduleDebitCommand {
    private final DebitAccountEvent message;

    @JsonCreator
    public ScheduleDebitCommand(@JsonProperty("message") DebitAccountEvent message) {
        this.message = message;
    }

    public DebitAccountEvent getMessage() {
        return message;
    }
}
