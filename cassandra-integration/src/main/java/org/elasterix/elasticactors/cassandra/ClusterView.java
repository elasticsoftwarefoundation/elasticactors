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

package org.elasterix.elasticactors.cassandra;

import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.net.InetAddress;
import java.util.List;

/**
 *
 */
public class ClusterView implements IEndpointLifecycleSubscriber {
    private static final Logger log = Logger.getLogger(ClusterView.class);
    private NodeProbe nodeProbe;

    @Override
    public void onJoinCluster(InetAddress endpoint) {
        log.info(String.format("%s joined the cluster", endpoint.getHostName()));
    }

    @Override
    public void onLeaveCluster(InetAddress endpoint) {
        log.info(String.format("%s left the cluster", endpoint.getHostName()));
    }

    @Override
    public void onUp(InetAddress endpoint) {
        log.info("************************** ElasticActors Node Marked UP ****************************");
        log.info(String.format("%s is now UP", endpoint.getHostName()));
        try {
            nodeProbe = new NodeProbe("localhost");
        } catch (Exception e) {
            log.error("Exception starting ApplicationContext & NodeProbe", e);
        }
        log.info(String.format("localNode id = %s", nodeProbe.getLocalHostId()));
        List<InetAddress> naturalEndpoints = nodeProbe.getEndpoints("ElasticActors", "ActorSystems", "testKey");
        log.info(String.format("Primary endpoint for key 'testKey' is %s ", naturalEndpoints.get(0).getHostName()));

    }

    @Override
    public void onDown(InetAddress endpoint) {
        log.info(String.format("%s is now DOWN", endpoint.getHostName()));
    }

    @Override
    public void onMove(InetAddress endpoint) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
