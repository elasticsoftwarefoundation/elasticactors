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

package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.*;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

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
        when(internalActorSystems.createPersistentActorRef(shard,"master")).thenReturn(new ActorShardRef("LocalNode",shard,"master"));
        ActorRef actorRef = ActorRefTools.parse("actor://LocalNode/Pi/shards/0/master",internalActorSystems);
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
        when(internalActorSystems.createPersistentActorRef(shard,null)).thenReturn(new ActorShardRef("LocalNode",shard,null));
        ActorRef actorRef = ActorRefTools.parse("actor://LocalNode/Pi/shards/0",internalActorSystems);
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
        when(internalActorSystems.createServiceActorRef(node,"pi/calculate")).thenReturn(new ServiceActorRef("LocalCluster",node,"pi/calculate"));
        ActorRef serviceRef = ActorRefTools.parse(serviceRefString,internalActorSystems);
        assertNotNull(serviceRef);
        assertEquals(serviceRef.getActorId(),"pi/calculate");
        assertEquals(serviceRef.toString(),serviceRefString);
    }

    @Test
    public void testParseServiceActorRefOnAnotherNodeThatsNotYetJoinedMyClusterView() {
        // /trading.service.GGM
        String nodeId = "trading001.dev.getbux.com";
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        InternalActorSystem actorSystem = mock(InternalActorSystem.class);
        ActorNode localNode = mock(ActorNode.class);
        ActorNode remoteNode = mock(ActorNode.class);
        NodeKey nodeKey = new NodeKey("trading",nodeId);
        NodeKey remoteNodeKey = new NodeKey("trading","trading002.dev.getbux.com");
        when(internalActorSystems.getClusterName()).thenReturn("trading.dev.getbux.com");
        when(internalActorSystems.get("trading")).thenReturn(actorSystem);
        when(actorSystem.getNode("trading002.dev.getbux.com")).thenReturn(remoteNode);
        when(actorSystem.getNode()).thenReturn(localNode);
        when(localNode.getKey()).thenReturn(nodeKey);
        when(remoteNode.getKey()).thenReturn(remoteNodeKey);
        String serviceRefString = String.format("actor://trading.dev.getbux.com/trading/services/%s/trading.service.GGM","trading002.dev.getbux.com");
        when(internalActorSystems.createServiceActorRef(localNode,"trading.service.GGM")).thenReturn(new ServiceActorRef("trading.dev.getbux.com",localNode,"trading.service.GGM"));
        when(internalActorSystems.createServiceActorRef(remoteNode,"trading.service.GGM")).thenReturn(new ServiceActorRef("trading.dev.getbux.com",remoteNode,"trading.service.GGM"));
        ActorRef serviceRef = ActorRefTools.parse(serviceRefString,internalActorSystems);
        assertNotNull(serviceRef);
        assertEquals(serviceRef.getActorId(),"trading.service.GGM");
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
        when(internalActorSystems.createServiceActorRef(node,"pi/calculate/with/multiple/slashes")).thenReturn(new ServiceActorRef("LocalCluster",node,"pi/calculate/with/multiple/slashes"));
        ActorRef serviceRef = ActorRefTools.parse(serviceRefString,internalActorSystems);
        assertNotNull(serviceRef);
        assertEquals(serviceRef.getActorId(),"pi/calculate/with/multiple/slashes");
        assertEquals(serviceRef.toString(),serviceRefString);
    }

    @Test
    public void testParseServiceActorRefWithOldFormat() {
        String nodeId = UUID.randomUUID().toString();
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        InternalActorSystem actorSystem = mock(InternalActorSystem.class);
        ActorNode node = mock(ActorNode.class);
        NodeKey nodeKey = new NodeKey("Pi",nodeId);
        when(internalActorSystems.getClusterName()).thenReturn("LocalCluster");
        when(internalActorSystems.get("Pi")).thenReturn(actorSystem);
        when(actorSystem.getNode("pi")).thenReturn(null);
        when(actorSystem.getNode()).thenReturn(node);
        when(node.getKey()).thenReturn(nodeKey);
        String serviceRefString = String.format("actor://LocalCluster/Pi/services/%s/pi/calculate",nodeId);
        String oldServiceRefString = "actor://LocalCluster/Pi/services/pi/calculate";
        when(internalActorSystems.createServiceActorRef(node,"pi/calculate")).thenReturn(new ServiceActorRef("LocalCluster",node,"pi/calculate"));
        ActorRef serviceRef = ActorRefTools.parse(oldServiceRefString,internalActorSystems);
        assertNotNull(serviceRef);
        assertEquals(serviceRef.getActorId(),"pi/calculate");
        assertEquals(serviceRef.toString(),serviceRefString);
    }

    @Test
    public void testParseServiceActorRefWithOldFormatWithoutSlashInActorId() {
        String nodeId = UUID.randomUUID().toString();
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        InternalActorSystem actorSystem = mock(InternalActorSystem.class);
        ActorNode node = mock(ActorNode.class);
        NodeKey nodeKey = new NodeKey("Pi",nodeId);
        when(internalActorSystems.getClusterName()).thenReturn("LocalCluster");
        when(internalActorSystems.get("Pi")).thenReturn(actorSystem);
        when(actorSystem.getNode("pi")).thenReturn(null);
        when(actorSystem.getNode()).thenReturn(node);
        when(node.getKey()).thenReturn(nodeKey);
        String serviceRefString = String.format("actor://LocalCluster/Pi/services/%s/calculate",nodeId);
        String oldServiceRefString = "actor://LocalCluster/Pi/services/calculate";
        when(internalActorSystems.createServiceActorRef(node,"calculate")).thenReturn(new ServiceActorRef("LocalCluster",node,"calculate"));
        ActorRef serviceRef = ActorRefTools.parse(oldServiceRefString,internalActorSystems);
        assertNotNull(serviceRef);
        assertEquals(serviceRef.getActorId(),"calculate");
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
        ActorRef actorRef = ActorRefTools.parse("actor://RemoteCluster/Pi/shards/0",internalActorSystems);
        assertNotNull(actorRef);
        assertNull(actorRef.getActorId());
        assertEquals(actorRef.toString(), "actor://RemoteCluster/Pi/shards/0");
    }
}