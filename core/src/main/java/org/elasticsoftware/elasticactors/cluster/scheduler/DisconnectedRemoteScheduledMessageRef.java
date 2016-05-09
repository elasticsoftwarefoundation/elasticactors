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

import org.elasticsoftware.elasticactors.ActorContainer;
import org.elasticsoftware.elasticactors.ActorContainerRef;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public final class DisconnectedRemoteScheduledMessageRef implements ScheduledMessageRef, ActorContainerRef {
    private final String clusterName;
    private final ScheduledMessageKey scheduledMessageKey;
    private final String refSpec;

    public DisconnectedRemoteScheduledMessageRef(String clusterName, String actorSystemName, int shardId, ScheduledMessageKey scheduledMessageKey) {
        this.clusterName = clusterName;
        this.scheduledMessageKey = scheduledMessageKey;
        this.refSpec = format(REFSPEC_FORMAT,clusterName,actorSystemName,shardId,scheduledMessageKey.getFireTime(),scheduledMessageKey.getId().toString());
    }

    @Override
    public void cancel() {
        throw new IllegalStateException(format("Remote Actor Cluster %s is not configured, ensure a correct remote configuration in the config.yaml",clusterName));
    }

    @Override
    public long getFireTime() {
        return scheduledMessageKey.getFireTime();
    }

    @Override
    public ActorContainer getActorContainer() {
        throw new IllegalStateException(format("Remote Actor Cluster %s is not configured, ensure a correct remote configuration in the config.yaml",clusterName));
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
