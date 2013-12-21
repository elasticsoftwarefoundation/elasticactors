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

package org.elasticsoftware.elasticactors.cassandra.state;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.UntypedActor;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
//import org.elasticsoftware.elasticactors.serialization.internal.PersistentActorDeserializer;
//import org.elasticsoftware.elasticactors.serialization.internal.PersistentActorSerializer;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

/**
 * @author Joost van de Wijgerd
 */
@ContextConfiguration(locations = {"classpath:cluster-beans.xml"})
public class CassandraPersistentActorRepositoryTest extends AbstractTestNGSpringContextTests {
    @Inject
    private CassandraPersistentActorRepository repository;
    /*@Inject
    private PersistentActorDeserializer persistentActorDeserializer;
    @Inject
    private PersistentActorSerializer persistentActorSerializer;*/

    @Test(enabled = false)
    public void testUpdate() throws IOException {
        InternalActorSystems cluster = mock(InternalActorSystems.class);
        ActorRefFactory actorRefFactory = mock(ActorRefFactory.class);
        InternalActorSystem actorSystem = mock(InternalActorSystem.class);
        ActorRef actorRef = mock(ActorRef.class);
        // override for test purposes
        /*persistentActorDeserializer.setActorRefFactory(actorRefFactory);
        persistentActorDeserializer.setActorSystems(cluster);
        persistentActorSerializer.setActorSystems(cluster);*/

        ShardKey shardKey = new ShardKey("IntegrationTest",0);

        when(cluster.get("IntegrationTest")).thenReturn(actorSystem);
        when(actorRef.getActorId()).thenReturn("testActor");
        when(actorRef.toString()).thenReturn("actor://TestCluster/IntegrationTest/shards/0/testActor");
        when(actorRefFactory.create("actor://TestCluster/IntegrationTest/shards/0/testActor")).thenReturn(actorRef);
        repository.update(shardKey,new PersistentActor(shardKey,actorSystem, "1.0", actorRef, TestActor.class));

        assertTrue(repository.contains(shardKey,"testActor"));

        PersistentActor persistentActor = repository.get(shardKey,"testActor");
        assertNotNull(persistentActor);
        assertEquals(persistentActor.getState(),null);
        assertEquals(persistentActor.getPreviousActorSystemVersion(),"1.0");
        assertEquals(persistentActor.getKey(),shardKey);
        assertEquals(persistentActor.getActorClass(),TestActor.class);
        assertEquals(persistentActor.getActorSystem(),actorSystem);
        assertEquals(persistentActor.getSelf(),actorRef);

        // remove
        repository.delete(shardKey,"testActor");

        assertFalse(repository.contains(shardKey, "testActor"));


    }

    public static final class TestActor extends UntypedActor {

        @Override
        public void onReceive(ActorRef sender, Object message) throws Exception {
            // do nothing
        }
    }




}
