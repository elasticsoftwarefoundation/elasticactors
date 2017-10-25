package org.elasticsoftware.elasticactors.eventsourcing;

import java.math.BigDecimal;

public interface VirtualBankAccount {
    String getIban();

    String getCurrency();

    BigDecimal getBalance();
}
