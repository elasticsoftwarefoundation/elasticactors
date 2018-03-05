package org.elasticsoftware.elasticactors.kafka.testapp.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Message;

import java.math.BigDecimal;

@Message(serializationFramework = JacksonSerializationFramework.class, immutable = true)
public class DebitAccountEvent {
    private final BigDecimal amount;

    @JsonCreator
    public DebitAccountEvent(@JsonProperty("amount") BigDecimal amount) {
        this.amount = amount;
    }

    public BigDecimal getAmount() {
        return amount;
    }
}
