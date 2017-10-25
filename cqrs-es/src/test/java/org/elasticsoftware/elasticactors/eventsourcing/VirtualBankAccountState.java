package org.elasticsoftware.elasticactors.eventsourcing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.state.JacksonActorState;

import java.math.BigDecimal;

public final class VirtualBankAccountState extends JacksonActorState<VirtualBankAccountState> implements VirtualBankAccount {
    private final String iban;
    private final String currency;
    private BigDecimal balance;

    public VirtualBankAccountState(String iban, String currency) {
        this(iban, currency, new BigDecimal("0.00"));
    }

    @JsonCreator
    public VirtualBankAccountState(@JsonProperty("iban") String iban,
                                   @JsonProperty("currency") String currency,
                                   @JsonProperty("balance") BigDecimal balance) {
        this.iban = iban;
        this.currency = currency;
        this.balance = balance;
    }

    @Override
    public String getIban() {
        return iban;
    }

    @Override
    public String getCurrency() {
        return currency;
    }

    @Override
    public BigDecimal getBalance() {
        return balance;
    }

    public void debitBalance(BigDecimal amount) {
        this.balance = this.balance.subtract(amount);
    }

    public void creditBalance(BigDecimal amount) {
        this.balance = this.balance.add(amount);
    }


    @Override
    public VirtualBankAccountState getBody() {
        return this;
    }
}
