/*
 * Copyright 2013 - 2014 The Original Authors
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

import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.messaging.internal.CancelScheduledMessageMessage;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class ScheduledMessageShardRef implements ScheduledMessageRef, ActorContainerRef {
    private final ActorShard shard;
    private final ScheduledMessageKey scheduledMessageKey;
    private final String refSpec;

    public ScheduledMessageShardRef(String clusterName, ActorShard shard, ScheduledMessageKey scheduledMessageKey) {
        this.shard = shard;
        this.scheduledMessageKey = scheduledMessageKey;
        this.refSpec = format(REFSPEC_FORMAT,clusterName,shard.getKey().getActorSystemName(),shard.getKey().getShardId(),scheduledMessageKey.getFireTime(),scheduledMessageKey.getId().toString());
    }

    @Override
    public ActorContainer get() {
        return shard;
    }

    @Override
    public void cancel() throws Exception {
        // try to determine the sender if possible
        final ActorRef sender = ActorContextHolder.getSelf();
        final CancelScheduledMessageMessage cancelMessage = new CancelScheduledMessageMessage(scheduledMessageKey.getId(),scheduledMessageKey.getFireTime());
        this.shard.sendMessage(sender,shard.getActorRef(),cancelMessage);
    }

    @Override
    public long getFireTime() {
        return scheduledMessageKey.getFireTime();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if(!(o instanceof ScheduledMessageRef)) return false;
        ScheduledMessageRef that = (ScheduledMessageRef) o;
        return that.toString().equals(this.toString());
    }

    @Override
    public int hashCode() {
        return this.refSpec.hashCode();
    }

    @Override
    public String toString() {
        return this.refSpec;
    }
}
