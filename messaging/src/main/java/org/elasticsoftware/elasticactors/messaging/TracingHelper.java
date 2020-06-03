package org.elasticsoftware.elasticactors.messaging;

import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorRef;

final class TracingHelper {

    private TracingHelper() {
    }

    static String findRealSender() {
        ActorRef self = ActorContextHolder.getSelf();
        if (self != null) {
            return self.toString();
        }
        return null;
    }
}
