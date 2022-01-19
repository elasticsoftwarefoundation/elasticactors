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

package org.elasticsoftware.elasticactors.serialization.internal;

import com.google.protobuf.ByteString;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.serialization.SerializationFrameworks;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;
import org.elasticsoftware.elasticactors.state.PersistentActor;

import java.io.IOException;

import static org.elasticsoftware.elasticactors.util.SerializationTools.serializeActorState;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentActorSerializer implements Serializer<PersistentActor<ShardKey>,byte[]> {
    private final SerializationFrameworks serializationFrameworks;

    public PersistentActorSerializer(SerializationFrameworks serializationFrameworks) {
        this.serializationFrameworks = serializationFrameworks;
    }

    @Override
    public byte[] serialize(PersistentActor<ShardKey> persistentActor) throws IOException {
        Elasticactors.PersistentActor.Builder builder = Elasticactors.PersistentActor.newBuilder();
        builder.setShardKey(persistentActor.getKey().toString());
        builder.setActorSystemVersion(persistentActor.getCurrentActorStateVersion());
        builder.setActorClass(persistentActor.getActorClass().getName());
        builder.setActorRef(persistentActor.getSelf().toString());

        if (persistentActor.getState() != null) {
            builder.setState(ByteString.copyFrom(getSerializedState(persistentActor)));
        }

        if(persistentActor.getPersistentSubscriptions() != null && !persistentActor.getPersistentSubscriptions().isEmpty()) {
            persistentActor.getPersistentSubscriptions().forEach(s -> builder.addSubscriptions(
                    Elasticactors.Subscription.newBuilder()
                            .setPublisherRef(s.getPublisherRef().toString())
                            .setMessageName(s.getMessageName())
                            .setCancelled(s.isCancelled())));
        }

        if(persistentActor.getMessageSubscribers() != null && !persistentActor.getMessageSubscribers().isEmpty()) {
            persistentActor.getMessageSubscribers().forEach((messageName, messageSubscriber) ->
                builder.addSubscribers(Elasticactors.Subscriber.newBuilder()
                    .setMessageName(messageName)
                    .setSubscriberRef(messageSubscriber.getSubscriberRef().toString())
                    .setLeases(messageSubscriber.getLeases())));
        }

        return builder.build().toByteArray();
    }

    private byte[] getSerializedState(PersistentActor<ShardKey> persistentActor) throws IOException {
        // if the state was already serialized, assume it's the most recent to avoid duplicate serialization
        return persistentActor.getSerializedState() != null
            ? persistentActor.getSerializedState()
            : serializeActorState(
                serializationFrameworks,
                persistentActor.getActorClass(),
                persistentActor.getState()
            );
    }
}
