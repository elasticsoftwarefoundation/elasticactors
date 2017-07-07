/*
 * Copyright 2013 - 2017 The Original Authors
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

package org.elasticsoftware.elasticactors.test.configuration;

import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.cluster.LocalActorSystemInstance;
import org.elasticsoftware.elasticactors.cluster.scheduler.SchedulerService;
import org.elasticsoftware.elasticactors.cluster.strategies.SingleNodeScaleUpStrategy;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class SystemInitializer {
    private final PhysicalNode localNode;
    private final LocalActorSystemInstance localActorSystemInstance;
    private final ClusterService clusterService;
    private final SchedulerService schedulerService;

    public SystemInitializer(PhysicalNode localNode, LocalActorSystemInstance localActorSystemInstance,
                             ClusterService clusterService, SchedulerService schedulerService) {
        this.localNode = localNode;
        this.localActorSystemInstance = localActorSystemInstance;
        this.clusterService = clusterService;
        this.schedulerService = schedulerService;
    }

    @PostConstruct
    public void initialize() throws Exception {
        final List<PhysicalNode> localNodes = Arrays.<PhysicalNode>asList(localNode);
        localActorSystemInstance.updateNodes(localNodes);
        localActorSystemInstance.distributeShards(localNodes,new SingleNodeScaleUpStrategy(schedulerService));
        // signal master elected
        clusterService.reportReady();
    }
}
