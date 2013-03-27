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

package org.elasterix.elasticactors.cluster.cassandra;

import org.apache.cassandra.tools.NodeProbe;
import org.elasterix.elasticactors.PhysicalNode;
import org.elasterix.elasticactors.ShardKey;
import org.elasterix.elasticactors.cluster.NodeSelector;

import java.net.InetAddress;
import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class CassandraNodeSelector implements NodeSelector {
    private final NodeProbe nodeProbe;
    private final List<PhysicalNode> clusterNodes;

    public CassandraNodeSelector(NodeProbe nodeProbe, List<PhysicalNode> clusterNodes) {
        this.nodeProbe = nodeProbe;
        this.clusterNodes = clusterNodes;
    }

    @Override
    public List<PhysicalNode> getAll() {
        return clusterNodes;
    }

    @Override
    public PhysicalNode getPrimary(ShardKey shardKey) {
        List<InetAddress> naturalEndpoints = nodeProbe.getEndpoints("ElasticActors", "MessageQueues", shardKey.toString());
        // find the first endpoint that is currently live
        for (InetAddress naturalEndpoint : naturalEndpoints) {
            for (PhysicalNode clusterNode : clusterNodes) {
                if(clusterNode.getAddress().equals(naturalEndpoint)) {
                    return clusterNode;
                }
            }
        }
        // if we get here there are no live nodes for this shardKey, this is BAD!
        // @todo: throw a proper exception here
        throw new RuntimeException("Could not find active endpoint!");
    }
}
