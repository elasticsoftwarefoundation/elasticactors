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

import org.elasticsoftware.elasticactors.cluster.RegisteredActorSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Joost van de Wijgerd
 */
@ContextConfiguration(locations = {"classpath:cluster-beans.xml"})
public class CassandraActorSystemRepositoryTest extends AbstractTestNGSpringContextTests {
    @Autowired
    private CassandraActorSystemRepository repository;

    @Test(enabled = false)
    public void testFindAll() {
        List<RegisteredActorSystem> registeredActorSystems = repository.findAll();
        assertNotNull(registeredActorSystems);
        assertEquals(registeredActorSystems.size(),1);
        RegisteredActorSystem PiTest = registeredActorSystems.get(0);
        assertEquals(PiTest.getConfigurationClass(),"org.elasticsoftware.elasticactors.examples.pi.PiApproximator");
        assertEquals(PiTest.getName(),"PiTest");
        assertEquals(PiTest.getNrOfShards(),8);
    }

    @Test(enabled = false)
    public void testFindByName() {
        RegisteredActorSystem PiTest = repository.findByName("PiTest");
        assertNotNull(PiTest);
        assertEquals(PiTest.getConfigurationClass(),"org.elasticsoftware.elasticactors.examples.pi.PiApproximator");
        assertEquals(PiTest.getName(),"PiTest");
        assertEquals(PiTest.getNrOfShards(),8);
    }
}
