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

package org.elasticsoftware.elasticactors.geoevents;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.geoevents.actors.TestActor;
import org.elasticsoftware.elasticactors.geoevents.messages.PublishLocation;
import org.elasticsoftware.elasticactors.geoevents.messages.RegisterInterest;
import org.elasticsoftware.elasticactors.test.TestActorSystem;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertTrue;

/**
 * @author Joost van de Wijgerd
 */
public class GeoEventsActorSystemTest {
    private static final Logger logger = Logger.getLogger(GeoEventsActorSystemTest.class);

    private TestActorSystem testActorSystem;

        @BeforeMethod(enabled = true)
        public void setUp() {
            BasicConfigurator.resetConfiguration();
            BasicConfigurator.configure();
            testActorSystem = new TestActorSystem();
            testActorSystem.initialize();
        }

        @AfterMethod(enabled = true)
        public void tearDown() {
            if(testActorSystem != null) {
                testActorSystem.destroy();
                testActorSystem = null;
            }
            BasicConfigurator.resetConfiguration();
        }

    @Test(enabled = false)
    public void testInContainer() throws Exception {
        ActorSystem geoEventsSystem = testActorSystem.getActorSystem("geo-events");
        ActorRef dispatcher = geoEventsSystem.serviceActorFor("geoEventsService");
        final CountDownLatch waitLatch = new CountDownLatch(1);
        ActorRef listener = geoEventsSystem.tempActorOf(TestActor.class, new Receiver() {
            @Override
            public void onReceive(ActorRef sender, Object message) throws Exception {
                //@todo: capture the messages
                logger.info("Got message");
                waitLatch.countDown();
            }
        });

        // register interest in the center of amsterdam (with a radius of 2500 metres)
        dispatcher.tell(new RegisterInterest(listener,new Coordinate(52.370216d,4.895168d),2500),listener);

        ActorRef publisher = geoEventsSystem.tempActorOf(TestActor.class, new Receiver() {
            @Override
            public void onReceive(ActorRef sender, Object message) throws Exception {
                //@todo: capture the messages
            }
        });

        // publish event at the eBuddy office
        Map<String,Object> customProperties = new LinkedHashMap<String,Object>();
        customProperties.put("name","Joost van de Wijgerd");
        dispatcher.tell(new PublishLocation(publisher,new Coordinate(52.364207d,4.891793d),3600,customProperties),publisher);

        // listener should now receive an update
        assertTrue(waitLatch.await(1, TimeUnit.MINUTES));



    }

    @Test(enabled = false)
    public void testScanQuery() throws Exception {
        /*ActorSystem geoEventsSystem = testActorSystem.create(new GeoEventsActorSystem());
        ActorSystem testSystem = testActorSystem.create(new GeoEventsTestActorSystem());

        ActorRef dispatcher = geoEventsSystem.serviceActorFor("geoEventsService");
        final CountDownLatch waitLatch = new CountDownLatch(1);

        ActorRef scanListener = testSystem.tempActorOf(TestActor.class, new Receiver() {
            @Override
            public void onReceive(ActorRef sender, Object message) throws Exception {
                //@todo: capture the messages
                logger.info("Got message");
                waitLatch.countDown();
            }
        });

        ActorRef publisher = testSystem.tempActorOf(TestActor.class, new Receiver() {
            @Override
            public void onReceive(ActorRef sender, Object message) throws Exception {
                //@todo: capture the messages
            }
        });

        // publish event at the eBuddy office
        Map<String,Object> customProperties = new LinkedHashMap<String,Object>();
        customProperties.put("name","Joost van de Wijgerd");
        dispatcher.tell(new PublishLocation(publisher,new Coordinate(52.364207d,4.891793d),3600,customProperties),publisher);

        // wait a while
        Thread.sleep(100);

        // now let's do a scan


        dispatcher.tell(new ScanRequest(new Coordinate(52.364207d,4.891793d),100),scanListener);

        // listener should now receive an update
        assertTrue(waitLatch.await(1, TimeUnit.MINUTES));*/
    }
}
