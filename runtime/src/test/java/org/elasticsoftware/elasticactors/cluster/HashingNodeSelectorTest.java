/*
 * Copyright 2013 - 2016 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.PhysicalNode;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * @author Joost van de Wijgerd
 */
public class HashingNodeSelectorTest {
    @Test
    public void testThreeNodes() throws UnknownHostException {
        List<PhysicalNode> clusterNodes =
                Arrays.<PhysicalNode>asList(
                        new PhysicalNodeImpl("c8b53fd9-4d95-43fc-a1f7-96ca9e305d4", InetAddress.getByName("192.168.56.1"), true),
                        new PhysicalNodeImpl("3bf98a1a-3c43-4d90-86d3-20c8f22d96c0", InetAddress.getByName("192.168.56.1"), false),
                        new PhysicalNodeImpl("45a3fad3-c823-42f9-b0e5-90370b232698", InetAddress.getByName("192.168.56.1"), false));

        HashingNodeSelector hashingNodeSelector = new HashingNodeSelector(clusterNodes);

        PhysicalNode node0 = hashingNodeSelector.getPrimary("default/shards/0");
        PhysicalNode node1 = hashingNodeSelector.getPrimary("default/shards/1");
        PhysicalNode node2 = hashingNodeSelector.getPrimary("default/shards/2");
        PhysicalNode node3 = hashingNodeSelector.getPrimary("default/shards/3");
        PhysicalNode node4 = hashingNodeSelector.getPrimary("default/shards/4");
        PhysicalNode node5 = hashingNodeSelector.getPrimary("default/shards/5");
        PhysicalNode node6 = hashingNodeSelector.getPrimary("default/shards/6");
        PhysicalNode node7 = hashingNodeSelector.getPrimary("default/shards/7");

        assertEquals(node0.getId(),"c8b53fd9-4d95-43fc-a1f7-96ca9e305d4");
        assertEquals(node1.getId(),"45a3fad3-c823-42f9-b0e5-90370b232698");
        assertEquals(node2.getId(),"45a3fad3-c823-42f9-b0e5-90370b232698");
        assertEquals(node3.getId(),"3bf98a1a-3c43-4d90-86d3-20c8f22d96c0");
        assertEquals(node4.getId(),"c8b53fd9-4d95-43fc-a1f7-96ca9e305d4");
        assertEquals(node5.getId(),"3bf98a1a-3c43-4d90-86d3-20c8f22d96c0");
        assertEquals(node6.getId(),"45a3fad3-c823-42f9-b0e5-90370b232698");
        assertEquals(node7.getId(),"45a3fad3-c823-42f9-b0e5-90370b232698");




    }
}
