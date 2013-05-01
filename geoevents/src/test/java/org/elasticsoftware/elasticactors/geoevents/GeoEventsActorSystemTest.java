package org.elasticsoftware.elasticactors.geoevents;

import ch.hsr.geohash.GeoHash;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ActorSystem;
import org.elasterix.elasticactors.test.TestActorSystem;
import org.elasticsoftware.elasticactors.geoevents.actors.Region;
import org.elasticsoftware.elasticactors.geoevents.actors.TestActor;
import org.elasticsoftware.elasticactors.geoevents.messages.PublishLocation;
import org.elasticsoftware.elasticactors.geoevents.messages.RegisterInterest;
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

    @BeforeMethod
    public void setUp() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();
    }

    @Test(enabled = true)
    public void testInContainer() throws Exception {
        ActorSystem geoEventsSystem = TestActorSystem.create(new GeoEventsActorSystem());
        ActorSystem testSystem = TestActorSystem.create(new GeoEventsTestActorSystem());

        // pre-create region (workaround for possible bug with undeliverable messages)
        //geoEventsSystem.actorOf("regions/u17",Region.class,new Region.State(GeoHash.fromGeohashString("u1f")));

        ActorRef dispatcher = geoEventsSystem.serviceActorFor("geoEventsService");
        final CountDownLatch waitLatch = new CountDownLatch(1);
        ActorRef listener = testSystem.tempActorOf(TestActor.class, new Receiver() {
            @Override
            public void onReceive(ActorRef sender, Object message) throws Exception {
                //@todo: capture the messages
                logger.info("Got message");
                waitLatch.countDown();
            }
        });

        // register interest in the center of amsterdam (with a radius of 2500 metres)
        dispatcher.tell(new RegisterInterest(listener,new Coordinate(52.370216d,4.895168d),2500),listener);

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

        // listener should now receive an update
        assertTrue(waitLatch.await(1, TimeUnit.MINUTES));


    }
}
