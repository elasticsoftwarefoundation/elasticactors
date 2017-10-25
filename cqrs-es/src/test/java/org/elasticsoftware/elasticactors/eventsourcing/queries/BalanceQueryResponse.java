package org.elasticsoftware.elasticactors.eventsourcing.queries;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsoftware.elasticactors.base.serialization.JacksonSerializationFramework;
import org.elasticsoftware.elasticactors.cqrs.QueryResponse;
import org.elasticsoftware.elasticactors.serialization.Message;

@Message(serializationFramework = JacksonSerializationFramework.class, immutable = true, durable = false)
public class BalanceQueryResponse implements QueryResponse {
    private final VirtualBankAccountAdapter virtualBankAccount;

    @JsonCreator
    public BalanceQueryResponse(@JsonProperty("virtualBankAccount") VirtualBankAccountAdapter virtualBankAccount) {
        this.virtualBankAccount = virtualBankAccount;
    }

    public VirtualBankAccountAdapter getVirtualBankAccount() {
        return virtualBankAccount;
    }
}
