package org.elasticsoftware.elasticactors.eventsourcing;

import org.elasticsoftware.elasticactors.cqrs.ReadOnlyState;

import java.math.BigDecimal;

public final class ReadOnlyVirtualBankAccountState implements VirtualBankAccount, ReadOnlyState {
    private final String iban;
    private final String currency;
    private final BigDecimal balance;

    public ReadOnlyVirtualBankAccountState(VirtualBankAccountState state) {
        this.iban = state.getIban();
        this.currency = state.getCurrency();
        this.balance = state.getBalance();
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
}
