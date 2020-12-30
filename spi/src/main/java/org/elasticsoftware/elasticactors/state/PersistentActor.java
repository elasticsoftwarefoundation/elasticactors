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

package org.elasticsoftware.elasticactors.state;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.PersistentSubscription;
import org.elasticsoftware.elasticactors.SingletonActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.reactivestreams.InternalPersistentSubscription;
import org.elasticsoftware.elasticactors.reactivestreams.ProcessorContext;
import org.reactivestreams.Subscription;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistentActor<K> implements ActorContext, ProcessorContext {
    private final K key;
    private final InternalActorSystem actorSystem;
    private transient final String currentActorStateVersion;
    private final String previousActorSystemVersion;
    private final Class<? extends ElasticActor> actorClass;
    private final ActorRef ref;
    @Nullable
    private final String affinityKey;
    private transient volatile byte[] serializedState;
    private volatile ActorState actorState;
    private HashMultimap<String, MessageSubscriber> messageSubscribers;
    private List<InternalPersistentSubscription> persistentSubscriptions;

    /**
     * This Constructor should be used when creating a new PersistentActor in memory
     *
     * @param key
     * @param actorSystem
     * @param previousActorStateVersion
     * @param ref
     * @param actorClass
     * @param actorState
     */
    public PersistentActor(K key, InternalActorSystem actorSystem, String previousActorStateVersion, ActorRef ref, Class<? extends ElasticActor> actorClass,  ActorState actorState) {
        this(key, actorSystem, previousActorStateVersion, previousActorStateVersion, actorClass, ref, null, null, actorState, null, null);
    }

    public PersistentActor(K key, InternalActorSystem actorSystem, String previousActorStateVersion, ActorRef ref, String affinityKey, Class<? extends ElasticActor> actorClass,  ActorState actorState) {
        this(key, actorSystem, previousActorStateVersion, previousActorStateVersion, actorClass, ref, affinityKey, null, actorState, null, null);
    }

    /**
     * This Constructor should be used when the PersistentActor is deserialized
     *
     * @param key
     * @param actorSystem
     * @param currentActorStateVersion
     * @param previousActorSystemVersion
     * @param ref
     * @param actorClass
     * @param serializedState
     */
    public PersistentActor(K key, InternalActorSystem actorSystem, String currentActorStateVersion,
                           String previousActorSystemVersion, ActorRef ref, Class<? extends ElasticActor> actorClass,
                           byte[] serializedState, HashMultimap<String, MessageSubscriber> messageSubscribers,
                           List<InternalPersistentSubscription> persistentSubscriptions) {
        this(key, actorSystem, currentActorStateVersion, previousActorSystemVersion, actorClass, ref, null,
                serializedState, null, messageSubscribers, persistentSubscriptions);
    }

    public PersistentActor(K key, InternalActorSystem actorSystem, String currentActorStateVersion,
                           String previousActorSystemVersion, ActorRef ref, Class<? extends ElasticActor> actorClass,
                           byte[] serializedState, String affinityKey,
                           HashMultimap<String, MessageSubscriber> messageSubscribers,
                           List<InternalPersistentSubscription> persistentSubscriptions) {
        this(key, actorSystem, currentActorStateVersion, previousActorSystemVersion, actorClass, ref, affinityKey,
                serializedState, null, messageSubscribers, persistentSubscriptions);
    }

    protected PersistentActor(K key, InternalActorSystem actorSystem, String currentActorStateVersion,
                              String previousActorSystemVersion, Class<? extends ElasticActor> actorClass,
                              ActorRef ref, @Nullable String affinityKey, byte[] serializedState, ActorState actorState,
                              HashMultimap<String, MessageSubscriber> messageSubscribers,
                              List<InternalPersistentSubscription> persistentSubscriptions) {
        if (actorClass != null) {
            SingletonActor singletonActor = actorClass.getAnnotation(SingletonActor.class);
            if (singletonActor != null && !singletonActor.value().equals(ref.getActorId())) {
                throw new IllegalArgumentException(String.format(
                        "Diverging ID for SingletonActor of type '%s'. Expected: '%s'. Found: '%s'",
                        actorClass.getName(),
                        singletonActor.value(),
                        ref.getActorId()));
            }
        }
        this.key = key;
        this.actorSystem = actorSystem;
        this.currentActorStateVersion = currentActorStateVersion;
        this.previousActorSystemVersion = previousActorSystemVersion;
        this.actorClass = actorClass;
        this.ref = ref;
        this.affinityKey = affinityKey;
        this.serializedState = serializedState;
        this.actorState = actorState;
        this.messageSubscribers = messageSubscribers;
        this.persistentSubscriptions = persistentSubscriptions;
    }

    public K getKey() {
        return key;
    }

    @Nullable
    public String getAffinityKey() {
        return affinityKey;
    }

    public String getCurrentActorStateVersion() {
        return currentActorStateVersion;
    }

    public String getPreviousActorStateVersion() {
        return previousActorSystemVersion;
    }

    public Class<? extends ElasticActor> getActorClass() {
        return actorClass;
    }

    @Override
    public ActorRef getSelf() {
        return ref;
    }

    @Nullable
    @Override
    public Class<?> getSelfType() {
        return actorClass;
    }

    @Override
    public <T extends ActorState> T getState(Class<T> stateClass) {
        return stateClass.cast(actorState);
    }

    public ActorState getState() {
        return actorState;
    }

    public byte[] getSerializedState() {
        return serializedState;
    }

    public void setSerializedState(byte[] serializedState) {
        this.serializedState = serializedState;
    }

    @Override
    public void setState(ActorState state) {
        this.actorState = state;
    }

    @Override
    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    @Override
    public Collection<PersistentSubscription> getSubscriptions() {
        return persistentSubscriptions != null ? ImmutableList.copyOf(persistentSubscriptions) : Collections.emptyList();
    }

    @Override
    public Map<String, Set<ActorRef>> getSubscribers() {
        if(messageSubscribers == null || messageSubscribers.isEmpty()) {
            return Collections.emptyMap();
        } else {
            Map<String, Set<ActorRef>> result = new HashMap<>();
            messageSubscribers.asMap().forEach((message, subscribers) -> {
                Set<ActorRef> subscriberSet = new HashSet<>();
                subscribers.forEach(subscriber -> subscriberSet.add(subscriber.getSubscriberRef()));
                result.put(message, subscriberSet);
            } );
            return result;
        }
    }

    @Override
    public void addSubscription(InternalPersistentSubscription persistentSubscription) {
        if(persistentSubscriptions == null) {
            persistentSubscriptions = new ArrayList<>();
        }
        persistentSubscriptions.add(persistentSubscription);
    }

    public Optional<InternalPersistentSubscription> getSubscription(String messageName, ActorRef publisherRef) {
        if(persistentSubscriptions != null) {
            return persistentSubscriptions.stream()
                        .filter(p -> p.getMessageName().equals(messageName) && p.getPublisherRef().equals(publisherRef))
                        .findFirst();
        }
        return Optional.empty();
    }

    public boolean removeSubscription(String messageName, ActorRef publisherRef) {
        return persistentSubscriptions != null && persistentSubscriptions.removeIf(next -> next.getMessageName().equals(messageName) && next.getPublisherRef().equals(publisherRef));
    }

    public void cancelSubscription(String messageName, ActorRef publisherRef) {
        if(persistentSubscriptions != null) {
            persistentSubscriptions.stream()
                    .filter( p -> p.getMessageName().equals(messageName) && p.getPublisherRef().equals(publisherRef))
                    .findFirst().ifPresent(Subscription::cancel);
        }
    }

    public void cancelAllSubscriptions() {
        if(persistentSubscriptions != null) {
            persistentSubscriptions.forEach(Subscription::cancel);
        }
    }

    public List<InternalPersistentSubscription> getPersistentSubscriptions() {
        return persistentSubscriptions;
    }

    public void addSubscriber(String messageName, MessageSubscriber messageSubscriber) {
        if(messageSubscribers == null) {
            messageSubscribers = HashMultimap.create();
        }
        messageSubscribers.put(messageName, messageSubscriber);
    }

    public boolean removeSubscriber(String messageName, MessageSubscriber messageSubscriber) {
        return messageSubscribers != null && messageSubscribers.remove(messageName, messageSubscriber);
    }

    public HashMultimap<String, MessageSubscriber> getMessageSubscribers() {
        return messageSubscribers;
    }
    public void serializeState() throws IOException {
        this.serializedState = actorSystem.getParent().getSerializationFramework(actorState.getSerializationFramework())
                .getActorStateSerializer(this.actorState).serialize(this.actorState);
    }

}
