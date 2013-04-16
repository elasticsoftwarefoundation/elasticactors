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

import com.google.common.base.Charsets;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.log4j.Logger;
import org.elasterix.elasticactors.PhysicalNode;
import org.elasterix.elasticactors.cluster.NodeSelector;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * @author Joost van de Wijgerd
 */
public final class CassandraNodeSelector implements NodeSelector {
    private static final Logger logger = Logger.getLogger(CassandraNodeSelector.class);
    private final NodeProbe nodeProbe;
    private final List<PhysicalNode> clusterNodes;
    private final NavigableMap<BigInteger, PhysicalNode> ketamaNodes = new TreeMap<BigInteger, PhysicalNode>();
    private final MessageDigest md5;

    public CassandraNodeSelector(NodeProbe nodeProbe, List<PhysicalNode> clusterNodes) {
        this.nodeProbe = nodeProbe;
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

        for (Map.Entry<String, String> tokenEntry : tokensToNodes.entrySet()) {
            if(nodeMap.containsKey(tokenEntry.getValue())) {
                ketamaNodes.put(new BigInteger(tokenEntry.getKey()),nodeMap.get(tokenEntry.getValue()));
            }
        }
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        }
    }

    @Override
    public List<PhysicalNode> getAll() {
        return clusterNodes;
    }

    @Override
    public PhysicalNode getPrimary(String shardKey) {
        // get the MD5 hash as a BigInteger
        md5.reset();
        BigInteger token = new BigInteger(md5.digest(shardKey.getBytes(Charsets.UTF_8)));
        if (!ketamaNodes.containsKey(token)) {
            token = ketamaNodes.ceilingKey(token);
            // if hash == null we need to wrap around
            if(token == null) {
                token = ketamaNodes.firstKey();
            }
        }
        return ketamaNodes.get(token);
    }
}
