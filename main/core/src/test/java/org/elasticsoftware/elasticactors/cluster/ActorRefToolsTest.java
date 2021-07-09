/*
 *   Copyright 2013 - 2019 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster;

import org.testng.annotations.Test;

import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * @author Joost van de Wijgerd
 */
public class ActorRefToolsTest {
    @Test
    public void testParseActorRef() {
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        InternalActorSystem actorSystem = mock(InternalActorSystem.class);
        InternalActorSystemConfiguration configuration = mock(InternalActorSystemConfiguration.class);
        ActorShard shard = mock(ActorShard.class);
        ShardKey shardKey = new ShardKey("Pi",0);
        when(internalActorSystems.getClusterName()).thenReturn("LocalNode");
        when(internalActorSystems.get("Pi")).thenReturn(actorSystem);
        when(actorSystem.getConfiguration()).thenReturn(configuration);
        when(configuration.getNumberOfShards()).thenReturn(1);
        when(actorSystem.getShard("Pi/shards/0")).thenReturn(shard);
        when(shard.getKey()).thenReturn(shardKey);
        ActorShardRef shardRef = new ActorShardRef("LocalNode",shard,"master", actorSystem);
        when(internalActorSystems.createPersistentActorRef(shard,"master")).thenReturn(shardRef);
        ActorRefTools actorRefTools = new ActorRefTools(internalActorSystems);
        ActorRef actorRef = actorRefTools.parse("actor://LocalNode/Pi/shards/0/master");
        assertNotNull(actorRef);
        assertEquals(actorRef.getActorId(),"master");
        assertEquals(actorRef.toString(),"actor://LocalNode/Pi/shards/0/master");
    }

    @Test
    public void testParseShardRef() {
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        InternalActorSystem actorSystem = mock(InternalActorSystem.class);
        InternalActorSystemConfiguration configuration = mock(InternalActorSystemConfiguration.class);
        ActorShard shard = mock(ActorShard.class);
        ShardKey shardKey = new ShardKey("Pi",0);
        when(internalActorSystems.getClusterName()).thenReturn("LocalNode");
        when(internalActorSystems.get("Pi")).thenReturn(actorSystem);
        when(actorSystem.getConfiguration()).thenReturn(configuration);
        when(configuration.getNumberOfShards()).thenReturn(1);
        when(actorSystem.getShard("Pi/shards/0")).thenReturn(shard);
        when(shard.getKey()).thenReturn(shardKey);
        ActorShardRef shardRef = new ActorShardRef(null, "LocalNode",shard);
        when(internalActorSystems.createPersistentActorRef(shard,null)).thenReturn(shardRef);
        ActorRefTools actorRefTools = new ActorRefTools(internalActorSystems);
        ActorRef actorRef = actorRefTools.parse("actor://LocalNode/Pi/shards/0");
        assertNotNull(actorRef);
        assertNull(actorRef.getActorId());
        assertEquals(actorRef.toString(), "actor://LocalNode/Pi/shards/0");
    }

    @Test
    public void testParseServiceActorRefWithSlashInActorId() {
        String nodeId = UUID.randomUUID().toString();
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        InternalActorSystem actorSystem = mock(InternalActorSystem.class);
        ActorNode node = mock(ActorNode.class);
        NodeKey nodeKey = new NodeKey("Pi",nodeId);
        when(internalActorSystems.getClusterName()).thenReturn("LocalCluster");
        when(internalActorSystems.get("Pi")).thenReturn(actorSystem);
        when(actorSystem.getNode(nodeId)).thenReturn(node);
        when(node.getKey()).thenReturn(nodeKey);
        String serviceRefString = String.format("actor://LocalCluster/Pi/services/%s/pi/calculate",nodeId);
        ServiceActorRef serviceActorRef = new ServiceActorRef(actorSystem, "LocalCluster",node,"pi/calculate");
        when(internalActorSystems.createServiceActorRef(node,"pi/calculate")).thenReturn(serviceActorRef);
        ActorRefTools actorRefTools = new ActorRefTools(internalActorSystems);
        ActorRef serviceRef = actorRefTools.parse(serviceRefString);
        assertNotNull(serviceRef);
        assertEquals(serviceRef.getActorId(),"pi/calculate");
        assertEquals(serviceRef.toString(),serviceRefString);
    }

    @Test
    public void testParseServiceActorRefOnAnotherNodeThatsNotYetJoinedMyClusterView() {
        String nodeId = "node001.elasticsoftwarefoundation.org";
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        InternalActorSystem actorSystem = mock(InternalActorSystem.class);
        ActorNode localNode = mock(ActorNode.class);
        ActorNode remoteNode = mock(ActorNode.class);
        NodeKey nodeKey = new NodeKey("test",nodeId);
        NodeKey remoteNodeKey = new NodeKey("test","node002.elasticsoftwarefoundation.org");
        when(internalActorSystems.getClusterName()).thenReturn("test.elasticsoftwarefoundation.org");
        when(internalActorSystems.get("test")).thenReturn(actorSystem);
        when(actorSystem.getNode("node002.elasticsoftwarefoundation.org")).thenReturn(remoteNode);
        when(actorSystem.getNode()).thenReturn(localNode);
        when(localNode.getKey()).thenReturn(nodeKey);
        when(remoteNode.getKey()).thenReturn(remoteNodeKey);
        String serviceRefString = String.format("actor://test.elasticsoftwarefoundation.org/test/services/%s/testService","node002.elasticsoftwarefoundation.org");
        ServiceActorRef localRef = new ServiceActorRef(actorSystem, "test.elasticsoftwarefoundation.org",localNode,"testService");
        ServiceActorRef remoteRef = new ServiceActorRef(actorSystem, "test.elasticsoftwarefoundation.org",remoteNode,"testService");
        when(internalActorSystems.createServiceActorRef(localNode,"testService")).thenReturn(localRef);
        when(internalActorSystems.createServiceActorRef(remoteNode,"testService")).thenReturn(remoteRef);
        ActorRefTools actorRefTools = new ActorRefTools(internalActorSystems);
        ActorRef serviceRef = actorRefTools.parse(serviceRefString);
        assertNotNull(serviceRef);
        assertEquals(serviceRef.getActorId(),"testService");
        assertEquals(serviceRef.toString(),serviceRefString);
    }

    @Test
    public void testParseServiceActorRefWithMultipleSlashesInActorId() {
        String nodeId = UUID.randomUUID().toString();
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        InternalActorSystem actorSystem = mock(InternalActorSystem.class);
        ActorNode node = mock(ActorNode.class);
        NodeKey nodeKey = new NodeKey("Pi",nodeId);
        when(internalActorSystems.getClusterName()).thenReturn("LocalCluster");
        when(internalActorSystems.get("Pi")).thenReturn(actorSystem);
        when(actorSystem.getNode(nodeId)).thenReturn(node);
        when(node.getKey()).thenReturn(nodeKey);
        String serviceRefString = String.format("actor://LocalCluster/Pi/services/%s/pi/calculate/with/multiple/slashes",nodeId);
        ServiceActorRef serviceActorRef = new ServiceActorRef(actorSystem, "LocalCluster",node,"pi/calculate/with/multiple/slashes");
        when(internalActorSystems.createServiceActorRef(node,"pi/calculate/with/multiple/slashes")).thenReturn(serviceActorRef);
        ActorRefTools actorRefTools = new ActorRefTools(internalActorSystems);
        ActorRef serviceRef = actorRefTools.parse(serviceRefString);
        assertNotNull(serviceRef);
        assertEquals(serviceRef.getActorId(),"pi/calculate/with/multiple/slashes");
        assertEquals(serviceRef.toString(),serviceRefString);
    }

    @Test
    public void testParseRemoteShardRef() {
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        ActorSystem actorSystem = mock(ActorSystem.class, withSettings().extraInterfaces(ShardAccessor.class));
        ActorShard shard = mock(ActorShard.class);
        ShardKey shardKey = new ShardKey("Pi",0);
        when(internalActorSystems.getClusterName()).thenReturn("LocalNode");
        when(internalActorSystems.getRemote("RemoteCluster","Pi")).thenReturn(actorSystem);
        when(((ShardAccessor) actorSystem).getShard("Pi/shards/0")).thenReturn(shard);
        when(shard.getKey()).thenReturn(shardKey);
        ActorRefTools actorRefTools = new ActorRefTools(internalActorSystems);
        ActorRef actorRef = actorRefTools.parse("actor://RemoteCluster/Pi/shards/0");
        assertNotNull(actorRef);
        assertNull(actorRef.getActorId());
        assertEquals(actorRef.toString(), "actor://RemoteCluster/Pi/shards/0");
    }
}