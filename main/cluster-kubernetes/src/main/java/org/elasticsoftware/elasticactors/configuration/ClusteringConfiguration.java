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

package org.elasticsoftware.elasticactors.configuration;

import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.kubernetes.cluster.KubernetesClusterService;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

public class ClusteringConfiguration {

    @Bean(name= "clusterService")
    public ClusterService createClusterService(Environment env) {
        String namespace = env.getProperty("ea.cluster.kubernetes.namespace", "default");
        String name = env.getRequiredProperty("ea.cluster.kubernetes.statefulsetName");
        String nodeId = env.getRequiredProperty("ea.node.id");
        Boolean useDesiredReplicas = env.getProperty(
                "ea.cluster.kubernetes.useDesiredReplicas",
                Boolean.class,
                Boolean.TRUE);
        return new KubernetesClusterService(namespace, name, nodeId, useDesiredReplicas);
    }
}
