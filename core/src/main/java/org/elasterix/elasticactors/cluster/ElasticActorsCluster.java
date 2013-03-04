/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.cluster;

import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ActorSystem;
import org.elasterix.elasticactors.ActorSystemConfiguration;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public class ElasticActorsCluster implements ActorRefFactory, ApplicationContextAware {
    private static final AtomicReference<ElasticActorsCluster> INSTANCE = new AtomicReference<ElasticActorsCluster>(null);
    private final ConcurrentMap<String,LocalActorSystemInstance> managedActorSystems = new ConcurrentHashMap<String,LocalActorSystemInstance>();
    private String clusterName;

    public static ElasticActorsCluster getInstance() {
        return INSTANCE.get();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        INSTANCE.set(applicationContext.getBean(ElasticActorsCluster.class));
    }

    ActorSystem getOrCreateActorSystem(String name,ActorSystemConfiguration configuration) {
        return null;
    }

    @Value("${elasticactors.cluster.name}")
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public ActorRef create(String refSpec) throws IllegalArgumentException {
        // refSpec should look like: actor://<cluster>/<actorSystem>/shards/<shardId>/<actorId>
        if(refSpec.startsWith("actor://")) {
            int actorSeparatorIndex = 8;
            for(int i = 0; i < 3; i++) {
                int nextIndex = refSpec.indexOf('/',actorSeparatorIndex);
                if(nextIndex == -1) {
                    throw new IllegalArgumentException(
                        String.format("Invalid ActorRef, required spec: [actor://<cluster>/<actorSystem>/shards/<shardId>/<actorId (optional)>, actual spec: [%s]",refSpec));
                } else {
                    actorSeparatorIndex = nextIndex;
                }
            }
            int nextIndex = refSpec.indexOf('/',actorSeparatorIndex);
            String actorId = (nextIndex == -1) ? null : refSpec.substring(nextIndex);
            actorSeparatorIndex = (nextIndex == -1) ? actorSeparatorIndex : nextIndex;
            String[] components = refSpec.substring(8,actorSeparatorIndex).split("/");
            if(components.length == 4) {
                String cluster = components[0];
                if(!this.clusterName.equals(cluster)) {
                    throw new IllegalArgumentException(String.format("Cluster [%s] is not Local Cluster [%s]",cluster,clusterName));
                }
                String actorSystemName = components[1];
                if(!"shards".equals(components[2])) {
                    throw new IllegalArgumentException(
                                            String.format("Invalid ActorRef, required spec: [actor://<cluster>/<actorSystem>/shards/<shardId>/<actorId (optional)>, actual spec: [%s]",refSpec));
                }
                int shardId = Integer.parseInt(components[3]);
                LocalActorSystemInstance actorSystem = managedActorSystems.get(actorSystemName);
                if(actorSystem == null) {
                    throw new IllegalArgumentException(String.format("Unknown ActorSystem: %s",actorSystemName));
                }
                if(shardId >= actorSystem.getNumberOfShards()) {
                    throw new IllegalArgumentException(String.format("Unknown shard %d for ActorSystem %s. Available shards: %d",shardId,actorSystemName,actorSystem.getNumberOfShards()));
                }
                return new LocalClusterActorRef(clusterName,actorSystem.getShard(shardId),actorId);
            } else {
                throw new IllegalArgumentException(
                    String.format("Invalid ActorRef, required spec: [actor://<cluster>/<actorSystem>/shards/<shardId>/<actorId (optional)>, actual spec: [%s]",refSpec));
            }
        } else {
            throw new IllegalArgumentException(
                                String.format("Invalid ActorRef, required spec: [actor://<cluster>/<actorSystem>/shards/<shardId>/<actorId (optional)>, actual spec: [%s]",refSpec));
        }
    }
}
