/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.serialization.internal;

import com.google.common.collect.HashMultimap;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.reactivestreams.InternalPersistentSubscription;
import org.elasticsoftware.elasticactors.reactivestreams.PersistentSubscriptionImpl;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.protobuf.Elasticactors;
import org.elasticsoftware.elasticactors.state.MessageSubscriber;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.util.ByteBufferUtils;
import org.reactivestreams.Subscriber;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentActorDeserializer implements Deserializer<ByteBuffer, PersistentActor<ShardKey>> {
    private final ActorRefFactory actorRefFactory;
    private final InternalActorSystems actorSystems;

    public PersistentActorDeserializer(ActorRefFactory actorRefFactory, InternalActorSystems actorSystems) {
        this.actorRefFactory = actorRefFactory;
        this.actorSystems = actorSystems;
    }

    @Override
    public PersistentActor<ShardKey> deserialize(ByteBuffer serializedObject) throws IOException {
        Elasticactors.PersistentActor protobufMessage = ByteBufferUtils.throwingApplyAndReset(
            serializedObject,
            Elasticactors.PersistentActor::parseFrom
        );
        final ShardKey shardKey = ShardKey.fromString(protobufMessage.getShardKey());
        try {
            Class<? extends ElasticActor> actorClass =
                (Class<? extends ElasticActor>) getClassHelper().forName(protobufMessage.getActorClass());
            final String currentActorStateVersion = actorSystems.getActorStateVersion(actorClass);
            final ActorRef selfRef = actorRefFactory.create(protobufMessage.getActorRef());
            HashMultimap<String, MessageSubscriber> messageSubscribers = protobufMessage.getSubscribersCount() > 0 ? HashMultimap.create() : null;

            if (protobufMessage.getSubscribersCount() > 0) {
                protobufMessage.getSubscribersList().forEach(s -> messageSubscribers.put(
                    s.getMessageName(),
                    new MessageSubscriber(
                        actorRefFactory.create(s.getSubscriberRef()),
                        s.getLeases()
                    )
                ));
            }
            List<InternalPersistentSubscription> persistentSubscriptions = null;

            if (protobufMessage.getSubscriptionsCount() > 0) {
                persistentSubscriptions = protobufMessage.getSubscriptionsList().stream()
                    .map(s -> new PersistentSubscriptionImpl(
                        selfRef,
                        actorRefFactory.create(s.getPublisherRef()),
                        s.getMessageName(),
                        s.getCancelled(),
                        materializeSubscriber(selfRef, actorClass, s.getMessageName())
                    )).collect(Collectors.toList());
            }

            return new PersistentActor<>(
                shardKey,
                actorSystems.get(shardKey.getActorSystemName()),
                currentActorStateVersion,
                protobufMessage.getActorSystemVersion(),
                selfRef,
                actorClass,
                !protobufMessage.getState().isEmpty()
                    ? protobufMessage.getState().toByteArray()
                    : null,
                messageSubscribers,
                persistentSubscriptions
            );
        } catch (ClassNotFoundException e) {
            throw new IOException("Exception deserializing PersistentActor", e);
        }
    }

    @Override
    public boolean isSafe() {
        return true;
    }

    private Subscriber materializeSubscriber(ActorRef actorRef, Class<? extends ElasticActor> actorClass, String messageName) {
        ElasticActor elasticActor = actorSystems.get(null).getActorInstance(actorRef, actorClass);
        // currently the messageName == messageClassName
        try {
            return elasticActor.asSubscriber(getClassHelper().forName(messageName));
        } catch (ClassNotFoundException e) {
            // did not find the message class, this should not happen but we now return the generic subscriber
            return elasticActor.asSubscriber(null);
        }
    }
}
