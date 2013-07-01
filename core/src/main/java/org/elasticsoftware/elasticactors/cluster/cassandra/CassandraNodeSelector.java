/*
 * Copyright 2013 the original authors
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

package org.elasticsoftware.elasticactors.cluster.cassandra;

import com.google.common.base.Charsets;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.NodeSelector;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author Joost van de Wijgerd
 */
public final class CassandraNodeSelector implements NodeSelector {
    private static final Logger logger = Logger.getLogger(CassandraNodeSelector.class);
    private final List<PhysicalNode> clusterNodes;
    private final NavigableMap<LongToken, PhysicalNode> ketamaNodes = new TreeMap<LongToken, PhysicalNode>();
    private final Murmur3Partitioner partitioner = new Murmur3Partitioner();

    public CassandraNodeSelector(NodeProbe nodeProbe, List<PhysicalNode> clusterNodes) {
        this.clusterNodes = clusterNodes;
        Map<String,PhysicalNode> nodeMap = new HashMap<String,PhysicalNode>();
        for (PhysicalNode clusterNode : clusterNodes) {
            nodeMap.put(clusterNode.getAddress().getHostAddress(),clusterNode);
        }
        /*
        logger.info("************************* NODES **************************");
        for (Map.Entry<String, PhysicalNode> nodeEntry : nodeMap.entrySet()) {
            logger.info(String.format("%s -> %s",nodeEntry.getKey(),nodeEntry.getValue()));
        }
        */

        Map<String,String> tokensToNodes = nodeProbe.getTokenToEndpointMap();
        /*
        logger.info("************************* TOKENS *************************");
        for (Map.Entry<String, String> tokenEntry : tokensToNodes.entrySet()) {
            logger.info(String.format("%s -> %s",tokenEntry.getKey(),tokenEntry.getValue()));
        }
        */
        logger.info(String.format("nodeMap.keys(): [%s]",nodeMap.keySet()));
        for (Map.Entry<String, String> tokenEntry : tokensToNodes.entrySet()) {
            if(nodeMap.containsKey(tokenEntry.getValue())) {
                ketamaNodes.put(new LongToken(Long.parseLong(tokenEntry.getKey())),nodeMap.get(tokenEntry.getValue()));
            }
        }
        logger.info(String.format("ketamaNodes has %d entries",ketamaNodes.size()));
    }

    @Override
    public List<PhysicalNode> getAll() {
        return clusterNodes;
    }

    @Override
    public PhysicalNode getPrimary(String shardKey) {
        final LongToken originalToken = partitioner.getToken(ByteBuffer.wrap(shardKey.getBytes(Charsets.UTF_8)));
        LongToken token = originalToken;
        if (!ketamaNodes.containsKey(token)) {
            token = ketamaNodes.ceilingKey(token);
            // if hash == null we need to wrap around
            if(token == null) {
                token = ketamaNodes.firstKey();
            }
        }
        PhysicalNode physicalNode = ketamaNodes.get(token);
        logger.info(String.format("for key [%s], generated token [%s], mapping to token [%s] and node [%s]",shardKey,originalToken,token,physicalNode));
        return physicalNode;
    }
}
