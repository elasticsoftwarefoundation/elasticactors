package org.elasticsoftware.elasticactors.kafka.testapp.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.serialization.Message;

import java.math.BigDecimal;

@Message(serializationFramework = JacksonSerializationFramework.class, immutable = true)
public class TransferCommand {
    private final BigDecimal amount;
    private final String currency;
    private final String fromAccount;
    private final String toAccount;
    private final String transactionId;

    @JsonCreator
    public TransferCommand(@JsonProperty("amount") BigDecimal amount,
                           @JsonProperty("currency") String currency,
                           @JsonProperty("fromAccount") String fromAccount,
                           @JsonProperty("toAccount") String toAccount,
                           @JsonProperty("transactionId") String transactionId) {

        this.amount = amount;
        this.currency = currency;
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.transactionId = transactionId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public String getFromAccount() {
        return fromAccount;
    }

    public String getToAccount() {
        return toAccount;
    }

    public String getTransactionId() {
        return transactionId;
    }
}
