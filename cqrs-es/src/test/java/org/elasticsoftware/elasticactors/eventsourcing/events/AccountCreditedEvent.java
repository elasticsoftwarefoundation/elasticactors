package org.elasticsoftware.elasticactors.eventsourcing.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.eventsourcing.SourcedEvent;

import java.math.BigDecimal;

// take cash out of the account
public class AccountCreditedEvent implements SourcedEvent {
    private final BigDecimal amount;

    @JsonCreator
    public AccountCreditedEvent(@JsonProperty("amount") BigDecimal amount) {
        this.amount = amount;
    }

    public BigDecimal getAmount() {
        return amount;
    }
}
