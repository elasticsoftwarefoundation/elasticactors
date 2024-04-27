/*
 * Copyright 2013 - 2024 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.client.messaging;

import org.elasticsoftware.elasticactors.serialization.Message;

/**
 * This interface aims to provide the same information {@link Message} does. The point here is to
 * allow thinner clients to interface with actor system without having to create classes for each
 * message type.
 *
 * <br><br>
 * <strong>
 * Classes implementing this interface should not be annotation with {@link Message}
 * </strong>
 * If it is, the values provided by the {@link Message} annotation will take precedence over the
 * ones provided by the object.
 */
public interface ActorSystemMessage {

    /**
     * Equivalent to {@link Message#durable()}
     */
    default boolean isDurable() {
        return true;
    }

    /**
     * Equivalent to {@link Message#timeout()}
     */
    default int getTimeout() {
        return Message.NO_TIMEOUT;
    }

    /**
     * Since Elastic Actors currently deserializes messages based on their class, this method should
     * return the name of the class which the payload represents on the receiving end.
     */
    String getPayloadClass();

    /**
     * The serialized payload data.
     */
    byte[] getPayload();

}
