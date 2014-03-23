package org.elasticsoftware.elasticactors.cluster.scheduler;

import org.elasticsoftware.elasticactors.ActorShard;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.InternalActorSystemConfiguration;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.cluster.ShardAccessor;
import org.elasticsoftware.elasticactors.scheduler.ScheduledMessageRef;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * @author Joost van de Wijgerd
 */
public class ScheduledMessageRefToolsTest {
    @Test
    public void testParseLocalRef() {
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        InternalActorSystem actorSystem = mock(InternalActorSystem.class);
        InternalActorSystemConfiguration configuration = mock(InternalActorSystemConfiguration.class);
        ActorShard shard = mock(ActorShard.class);
        ShardKey shardKey = new ShardKey("Pi",0);
        when(internalActorSystems.getClusterName()).thenReturn("LocalNode");
        when(internalActorSystems.get("Pi")).thenReturn(actorSystem);
        when(actorSystem.getConfiguration()).thenReturn(configuration);
        when(configuration.getNumberOfShards()).thenReturn(1);
        when(actorSystem.getShard(0)).thenReturn(shard);
        when(shard.getKey()).thenReturn(shardKey);

        ScheduledMessageRef messageRef = ScheduledMessageRefTools.parse("message://LocalNode/Pi/shards/0/1395574117/2321d4d0-b27e-11e3-a5e2-0800200c9a66", internalActorSystems);
        assertNotNull(messageRef);
        assertEquals(messageRef.toString(),"message://LocalNode/Pi/shards/0/1395574117/2321d4d0-b27e-11e3-a5e2-0800200c9a66");
    }

    @Test
    public void testParseRemoteRef() {
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        ActorSystem remoteActorSystem = mock(ActorSystem.class,withSettings().extraInterfaces(ShardAccessor.class));
        ActorShard shard = mock(ActorShard.class);
        ShardKey shardKey = new ShardKey("Pi",0);
        when(internalActorSystems.getClusterName()).thenReturn("LocalNode");
        when(internalActorSystems.getRemote("RemoteCluster","Pi")).thenReturn(remoteActorSystem);
        when(((ShardAccessor)remoteActorSystem).getShard(0)).thenReturn(shard);
        when(shard.getKey()).thenReturn(shardKey);

        ScheduledMessageRef messageRef = ScheduledMessageRefTools.parse("message://RemoteCluster/Pi/shards/0/1395574117/2321d4d0-b27e-11e3-a5e2-0800200c9a66", internalActorSystems);
        assertNotNull(messageRef);
        assertEquals(messageRef.toString(),"message://RemoteCluster/Pi/shards/0/1395574117/2321d4d0-b27e-11e3-a5e2-0800200c9a66");
        assertTrue(messageRef instanceof ScheduledMessageShardRef);
    }

    @Test()
    public void testParseDisconnectedRemoteRef() throws Exception {
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        when(internalActorSystems.getClusterName()).thenReturn("LocalNode");
        when(internalActorSystems.getRemote("RemoteCluster","Pi")).thenReturn(null);

        ScheduledMessageRef messageRef = ScheduledMessageRefTools.parse("message://RemoteCluster/Pi/shards/0/1395574117/2321d4d0-b27e-11e3-a5e2-0800200c9a66", internalActorSystems);
        assertNotNull(messageRef);
        assertEquals(messageRef.toString(),"message://RemoteCluster/Pi/shards/0/1395574117/2321d4d0-b27e-11e3-a5e2-0800200c9a66");
        assertTrue(messageRef instanceof DisconnectedRemoteScheduledMessageRef);
        try {
            messageRef.cancel();
            fail("messageRef.cancel() did not throw IllegalArgumentException");
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(),"Remote Actor Cluster RemoteCluster is not configured, ensure a correct remote configuration in the config.yaml");
        }
        try {
            ((DisconnectedRemoteScheduledMessageRef) messageRef).get();
            fail("messageRef.get() did not throw IllegalArgumentException");
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(),"Remote Actor Cluster RemoteCluster is not configured, ensure a correct remote configuration in the config.yaml");
        }
    }


}
