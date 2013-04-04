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

package org.elasticsoftwarefoundation.elasticactors.http;

import org.apache.log4j.BasicConfigurator;
import org.elasterix.elasticactors.ActorSystem;
import org.elasterix.elasticactors.test.TestActorSystem;
import org.elasticsoftwarefoundation.elasticactors.http.actors.User;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Joost van de Wijgerd
 */
public class HttpActorSystemTest {
    @BeforeMethod
    public void setUp() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();
    }

    @Test(enabled = false)
    public void testInContainer() throws Exception {
        ActorSystem httpSystem = TestActorSystem.create(new HttpActorSystem());
        ActorSystem testSystem = TestActorSystem.create(new HttpTestActorSystem());

        // create a couple of users
        testSystem.actorOf("users/1",User.class);
        testSystem.actorOf("users/2",User.class);
        testSystem.actorOf("users/3",User.class);

        // @todo: use
        Thread.sleep(600000L);
    }
}
