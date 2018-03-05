package org.elasticsoftware.elasticactors.kafka.testapp.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.state.JacksonActorState;

import java.math.BigDecimal;

public class VirtualCashAccountState extends JacksonActorState<VirtualCashAccountState> {
    private final String id;
    private final String currency;
    private final int decimals;
    private BigDecimal balance;

    @JsonCreator
    public VirtualCashAccountState(@JsonProperty("id") String id,
                                   @JsonProperty("currency") String currency,
                                   @JsonProperty("decimals") int decimals) {
        this.id = id;
        this.currency = currency;
        this.decimals = decimals;
        this.balance = BigDecimal.ZERO.setScale(decimals);
    }

    @Override
    public VirtualCashAccountState getBody() {
        return this;
    }

    public String getId() {
        return id;
    }

    public String getCurrency() {
        return currency;
    }

    public int getDecimals() {
        return decimals;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public void setBalance(BigDecimal balance) {
        this.balance = balance;
    }
}
