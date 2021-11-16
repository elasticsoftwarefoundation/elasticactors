/*
 *   Copyright 2013 - 2019 The Original Authors
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

package org.elasticsoftware.elasticactors.serialization.reactivestreams;

import org.elasticsoftware.elasticactors.messaging.reactivestreams.RequestMessage;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Reactivestreams;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Joost van de Wijgerd
 */
public final class RequestMessageDeserializer implements MessageDeserializer<RequestMessage> {

    @Override
    public RequestMessage deserialize(ByteBuffer serializedObject) throws IOException {
        // Using duplicate instead of asReadOnlyBuffer so implementations can optimize this in case
        // the original byte buffer has an array
        Reactivestreams.RequestMessage subscriptionMessage = Reactivestreams.RequestMessage.parseFrom(serializedObject.duplicate());
        return new RequestMessage(subscriptionMessage.getMessageName(), subscriptionMessage.getN());
    }

    @Override
    public Class<RequestMessage> getMessageClass() {
        return RequestMessage.class;
    }
}
