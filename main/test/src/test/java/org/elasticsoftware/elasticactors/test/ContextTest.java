package org.elasticsoftware.elasticactors.test;

import org.testng.annotations.Test;

public class ContextTest {

    @Test
    public void contextLoads() {
        TestActorSystem testActorSystem = new TestActorSystem(ContextTestConfiguration.class, true);
        testActorSystem.initialize();

        testActorSystem.destroy();
    }

}
