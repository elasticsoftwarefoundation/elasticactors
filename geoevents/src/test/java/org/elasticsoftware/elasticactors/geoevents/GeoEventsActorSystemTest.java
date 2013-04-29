package org.elasticsoftware.elasticactors.geoevents;

import org.apache.log4j.BasicConfigurator;
import org.elasterix.elasticactors.ActorRef;
import org.elasterix.elasticactors.ActorSystem;
import org.elasterix.elasticactors.test.TestActorSystem;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Joost van de Wijgerd
 */
public class GeoEventsActorSystemTest {
    @BeforeMethod
    public void setUp() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();
    }

    @Test(enabled = true)
    public void testInContainer() throws Exception {
        ActorSystem geoEventsSystem = TestActorSystem.create(new GeoEventsActorSystem());

        ActorRef dispatcher = geoEventsSystem.serviceActorFor("requestDispatcher");


    }
}
