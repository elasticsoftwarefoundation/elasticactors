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

package org.elasticsoftware.elasticactors.examples.httpclient;

import org.apache.log4j.BasicConfigurator;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.examples.httpclient.actors.HttpResponseListener;
import org.elasticsoftware.elasticactors.examples.httpclient.messages.HttpRequest;
import org.elasticsoftware.elasticactors.test.TestActorSystem;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Joost van de Wijgerd
 */
public class HttpClientActorSystemTest {
    private TestActorSystem testActorSystem;

        @BeforeMethod
        public void setUp() {
            BasicConfigurator.resetConfiguration();
            BasicConfigurator.configure();
            testActorSystem = TestActorSystem.create();
        }

        @AfterMethod
        public void tearDown() {
            if(testActorSystem != null) {
                testActorSystem.destroy();
                testActorSystem = null;
            }
            BasicConfigurator.resetConfiguration();
        }

    @Test
    public void testInContainer() throws Exception {
        /*ActorSystem httpClientSystem = testActorSystem.create(new HttpClientActorSystem());
        ActorRef httpClientRef = httpClientSystem.serviceActorFor("httpClient");
        ActorRef listenerRef = httpClientSystem.tempActorOf(HttpResponseListener.class, null);
        httpClientRef.tell(new HttpRequest("http://www.google.com/"),listenerRef);
        Thread.sleep(7500);*/
    }
}
