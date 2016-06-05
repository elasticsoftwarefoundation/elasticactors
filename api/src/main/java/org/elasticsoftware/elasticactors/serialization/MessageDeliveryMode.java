/*
 * Copyright 2013 - 2016 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.serialization;

/**
 * @author Joost van de Wijgerd
 */
public enum MessageDeliveryMode {
    /**
     * This setting used the system wide setting. This can be set using a system property and by default this
     * is set to {@link org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode#STRICT_ORDER}
     */
    SYSTEM_DEFAULT(1),
    /**
     * All messages from actor A to actor B are guaranteed to arrive in the same order. This essentially
     * means that all messages are sent using the messaging layer. This is the default (and least optimized)
     * MessageDeliveryMode
     */
    STRICT_ORDER(2),
    /**
     * messages from Actor A to actor B that have {@link Message#durable()} set to {@link Boolean#FALSE} are
     * sent over the in-jvm queues when actor A and actor B are located on the same JVM. If you send mixed
     * durable and non-durable messages to actor B from actor A this can lead to out-of-order delivery.
     * This method can be considerable faster than {@link org.elasticsoftware.elasticactors.serialization.MessageDeliveryMode#STRICT_ORDER}
     * since it doesn't require a network hop to the messaging layer.
     */
    LOCAL_NON_DURABLE_OPTIMIZED(3);

    private final int id;

    private MessageDeliveryMode(int id) {
        this.id = id;
    }

    public static MessageDeliveryMode findById(int order) {
        for (MessageDeliveryMode mode : values()) {
            if(mode.id == order) {
                return mode;
            }
        }
        return SYSTEM_DEFAULT;
    }

    public final int getId() {
        return id;
    }
}
