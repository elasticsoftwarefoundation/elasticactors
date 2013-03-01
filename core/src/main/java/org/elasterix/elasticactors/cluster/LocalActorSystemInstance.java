/*
 * Copyright (c) 2013 Joost van de Wijgerd <jwijgerd@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasterix.elasticactors.cluster;

import org.apache.log4j.Logger;
import org.elasterix.elasticactors.ActorShard;
import org.elasterix.elasticactors.ActorSystem;
import org.elasterix.elasticactors.PhysicalNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public class LocalActorSystemInstance {
    private static final Logger log = Logger.getLogger(LocalActorSystemInstance.class);
    private final ActorSystem actorSystem;
    private final ActorShard[] shards;
    private NodeSelectorFactory nodeSelectorFactory;

    public LocalActorSystemInstance(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
        this.shards = new ActorShard[actorSystem.getNumberOfShards()];
    }

    /**
     * Distribute the shards over the list of physical nodes
     *
     * @param nodes
     */
    public void distributeShards(List<PhysicalNode> nodes) {
        NodeSelector nodeSelector = nodeSelectorFactory.create(nodes);
        for(int i = 0; i < actorSystem.getNumberOfShards(); i++) {

        }
    }

    @Autowired
    public void setNodeSelectorFactory(NodeSelectorFactory nodeSelectorFactory) {
        this.nodeSelectorFactory = nodeSelectorFactory;
    }
}
