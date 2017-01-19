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

package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.cluster.ShardAccessor;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;

import java.util.UUID;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ScheduledMessageRefTools {
    private static final String EXCEPTION_FORMAT = "Invalid ScheduledMessageRef, required spec: [message://<cluster>/<actorSystem>/shards/<fireTime (long)>/<messageId (uuid)>, actual spec: [%s]";

    private ScheduledMessageRefTools() {}

    public static ScheduledMessageRef parse(String refSpec, InternalActorSystems cluster) {
        if(refSpec.startsWith("message://")) {
            final String[] components = refSpec.substring(10).split("/");
            // should have 6 components
            if(components.length == 6) {
                final ScheduledMessageKey key = new ScheduledMessageKey(UUID.fromString(components[5]),Long.parseLong(components[4]));
                final int shardId = Integer.parseInt(components[3]);
                if(components[0].equals(cluster.getClusterName())) {
                    // local ref
                    ActorShard localShard = cluster.get(components[1]).getShard(shardId);
                    return new ScheduledMessageShardRef(components[0],localShard,key);
                } else {
                    // remote ref
                    ActorSystem remoteActorSystem = cluster.getRemote(components[0],components[1]);
                    if(remoteActorSystem != null) {
                        // @todo: dirty but we know a remote actor system is also a ShardAccessor
                        ActorShard remoteShard = ((ShardAccessor)remoteActorSystem).getShard(shardId);
                        return new ScheduledMessageShardRef(components[0],remoteShard,key);
                    } else {
                        // unknown remote actorsystem, to ensure deserialization works we'll return an object here
                        // that will give IllegalStateExceptions on the API methods later
                        return new DisconnectedRemoteScheduledMessageRef(components[0],components[1],shardId,key);
                    }

                }
            } else {
                throw new IllegalArgumentException(format(EXCEPTION_FORMAT,refSpec));
            }
        } else {
            throw new IllegalArgumentException(format(EXCEPTION_FORMAT,refSpec));
        }
    }
}
