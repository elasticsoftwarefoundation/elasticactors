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

package org.elasticsoftware.elasticactors.configuration;

import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.shoal.cluster.ShoalClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;

/**
 * @author Joost van de Wijgerd
 */
public class ClusteringConfiguration {
    @Autowired
    private Environment env;

    @Bean(name= "clusterService")
    public ClusterService createClusterService() throws UnknownHostException {
        String nodeId = env.getRequiredProperty("ea.node.id");
        //@todo: fix node address
        InetAddress nodeAddress = InetAddress.getByName(env.getRequiredProperty("ea.node.address"));
        int nodePort = env.getProperty("ea.node.port",Integer.class,9090);
        String clusterName = env.getRequiredProperty("ea.cluster");
        String discoveryNodes = env.getProperty("ea.cluster.discovery.nodes",String.class,null);
        return new ShoalClusterService(clusterName,nodeId,nodeAddress,nodePort,discoveryNodes);
    }
}
