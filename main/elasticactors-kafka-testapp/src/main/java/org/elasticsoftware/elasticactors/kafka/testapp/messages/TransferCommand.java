/*
 *   Copyright 2013 - 2022 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

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
