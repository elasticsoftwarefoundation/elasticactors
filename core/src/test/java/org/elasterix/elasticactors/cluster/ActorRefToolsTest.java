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

package org.elasterix.elasticactors.cluster;

import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ActorShard;
import org.elasterix.elasticactors.ShardKey;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Joost van de Wijgerd
 */
public class ActorRefToolsTest {
    @Test
    public void testParseActorRef() {
        InternalActorSystems internalActorSystems = mock(InternalActorSystems.class);
        InternalActorSystem actorSystem = mock(InternalActorSystem.class);
        ActorShard shard = mock(ActorShard.class);
        ShardKey shardKey = new ShardKey("Pi",0);
        when(internalActorSystems.getClusterName()).thenReturn("LocalNode");
        when(internalActorSystems.get("Pi")).thenReturn(actorSystem);
        when(actorSystem.getNumberOfShards()).thenReturn(1);
        when(actorSystem.getShard(0)).thenReturn(shard);
        when(shard.getKey()).thenReturn(shardKey);
        ActorRef actorRef = ActorRefTools.parse("actor://LocalNode/Pi/shards/0/master",internalActorSystems);
        assertNotNull(actorRef);
        assertEquals(actorRef.getActorId(),"master");
        assertEquals(actorRef.toString(),"actor://LocalNode/Pi/shards/0/master");
    }


}
