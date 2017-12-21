package org.elasticsoftware.elasticactors.kafka.testapp.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Message;

import java.math.BigDecimal;

@Message(serializationFramework = JacksonSerializationFramework.class, immutable = true)
public class TransferCommand {
    private final BigDecimal amount;
    private final String fromAccount;
    private final String toAccount;

    @JsonCreator
    public TransferCommand(@JsonProperty("amount") BigDecimal amount,
                           @JsonProperty("fromAccount") String fromAccount,
                           @JsonProperty("toAccount") String toAccount) {
        this.amount = amount;
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getFromAccount() {
        return fromAccount;
    }

    public String getToAccount() {
        return toAccount;
    }
}
