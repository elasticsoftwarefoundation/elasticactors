package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.UntypedActor;

/**
 * @author Joost van de Wijgerd
 */
@Actor
public final class TestActor extends UntypedActor {
    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
