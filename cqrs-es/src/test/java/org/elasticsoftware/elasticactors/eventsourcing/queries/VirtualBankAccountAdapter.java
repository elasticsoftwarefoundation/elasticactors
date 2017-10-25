package org.elasticsoftware.elasticactors.eventsourcing.queries;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.eventsourcing.VirtualBankAccount;

import java.math.BigDecimal;

public class VirtualBankAccountAdapter implements VirtualBankAccount {
    private final String iban;
    private final String currency;
    private final BigDecimal balance;

    public VirtualBankAccountAdapter(VirtualBankAccount account) {
        this(account.getIban(), account.getCurrency(), account.getBalance());
    }

    @JsonCreator
    public VirtualBankAccountAdapter(@JsonProperty("iban") String iban,
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
}
