package org.elasticsoftware.elasticactors.eventsourcing.commands;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.cqrs.Command;
import org.elasticsoftware.elasticactors.serialization.Message;

import java.math.BigDecimal;

@Message(serializationFramework = JacksonSerializationFramework.class, immutable = true, durable = true)
public class CreditAccountCommand implements Command {
    private final String fromAccount;
    private final String toAccount;
    private final BigDecimal amount;

    @JsonCreator
    public CreditAccountCommand(@JsonProperty("fromAccount") String fromAccount,
                                @JsonProperty("toAccount") String toAccount,
                                @JsonProperty("amount") BigDecimal amount) {
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.amount = amount;
    }

    public String getFromAccount() {
        return fromAccount;
    }

    public String getToAccount() {
        return toAccount;
    }

    public BigDecimal getAmount() {
        return amount;
    }
}
