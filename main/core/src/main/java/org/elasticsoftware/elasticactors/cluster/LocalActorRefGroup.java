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

package org.elasticsoftware.elasticactors.cluster;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorRefGroup;
import org.elasticsoftware.elasticactors.MessageDeliveryException;

import java.util.Map;

import static org.elasticsoftware.elasticactors.ActorContextHolder.getSelf;

/**
 * @author Joost van de Wijgerd
 */
public final class LocalActorRefGroup implements ActorRefGroup {
    private final ImmutableListMultimap<ActorShardRef, ActorRef> members;

    public LocalActorRefGroup(ImmutableListMultimap<ActorShardRef, ActorRef> members) {
        this.members = members;
    }

    @Override
    public void tell(Object message, ActorRef sender) throws MessageDeliveryException {
        // go over all the buckets and compose the message
        for (ActorShardRef shardKey : members.keySet()) {
            // get the actorShard from the first member
            ImmutableList<ActorRef> receivers = members.get(shardKey);
            ActorContainer actorShard = shardKey.getActorContainer();
            try {
                actorShard.sendMessage(sender, receivers, message);
            } catch(MessageDeliveryException e) {
                throw e;
            } catch (Exception e) {
                throw new MessageDeliveryException("Unexpected Exception while sending message",e,false);
            }
        }
    }

    @Override
    public void tell(Object message) throws IllegalStateException, MessageDeliveryException {
        final ActorRef self = getSelf();
        if(self != null) {
            tell(message,self);
        } else {
            throw new IllegalStateException("Cannot determine ActorRef(self). Only use this method while inside an ElasticActor Lifecycle or on(Message) method!");
        }
    }

    @Override
    public ImmutableCollection<ActorRef> getMembers() {
        return members.values();
    }

    @Override
    public ActorRefGroup withMembersAdded(ActorRef... membersToAdd) throws IllegalArgumentException {
        for (ActorRef member : membersToAdd) {
            if(!(member instanceof ActorShardRef)) {
                throw new IllegalArgumentException("Only Persistent Actors (annotated with @Actor) of the same ElasticActors cluster are allowed to form a group");
            }
        }
        ImmutableListMultimap.Builder<ActorShardRef, ActorRef> newMemberMap = ImmutableListMultimap.builder();
        newMemberMap.putAll(this.members);
        for (ActorRef member : membersToAdd) {
            newMemberMap.put((ActorShardRef)((ActorShardRef)member).getActorContainer().getActorRef(), member);
        }
        return new LocalActorRefGroup(newMemberMap.build());
    }

    @Override
    public ActorRefGroup withMembersRemoved(ActorRef... membersToRemove) {
        // create a set for faster lookup
        ImmutableSet<ActorRef> memberToRemoveSet = ImmutableSet.copyOf(membersToRemove);
        ImmutableListMultimap.Builder<ActorShardRef, ActorRef> newMemberMap = ImmutableListMultimap.builder();
        for (Map.Entry<ActorShardRef,ActorRef> entry : this.members.entries()) {
            if(!memberToRemoveSet.contains(entry.getValue())) {
                newMemberMap.put(entry.getKey(), entry.getValue());
            }
        }
        return new LocalActorRefGroup(newMemberMap.build());
    }
}
