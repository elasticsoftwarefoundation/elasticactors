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

package org.elasterix.elasticactors.examples.httpclient;

import org.apache.log4j.BasicConfigurator;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ActorSystem;
import org.elasterix.elasticactors.examples.httpclient.actors.HttpResponseListener;
import org.elasterix.elasticactors.examples.httpclient.messages.HttpRequest;
import org.elasterix.elasticactors.test.TestActorSystem;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.UUID;

/**
 * @author Joost van de Wijgerd
 */
public class HttpClientActorSystemTest {
    @BeforeMethod
    public void setUp() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();
    }

    @Test
    public void testInContainer() throws Exception {
        ActorSystem httpClientSystem = TestActorSystem.create(new HttpClientActorSystem());
        ActorRef httpClientRef = httpClientSystem.serviceActorFor("httpClient");
        ActorRef listenerRef = httpClientSystem.tempActorOf(UUID.randomUUID().toString(), HttpResponseListener.class, null);
        httpClientRef.tell(new HttpRequest("http://www.google.com/"),listenerRef);
        Thread.sleep(7500);
    }
}
