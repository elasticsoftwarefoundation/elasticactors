package org.elasticsoftware.elasticactors.util;

import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.tracing.ExternalRealSenderDataHolder;
import org.elasticsoftware.elasticactors.tracing.RealSenderData;

import javax.annotation.Nullable;

public final class TracingHelper {

    private TracingHelper() {
    }

    private final static class InternalActorContextAccessor extends ActorContextHolder {
        private static ActorContext getCurrent() {
            return threadContext.get();
        }
    }

    @Nullable
    public static RealSenderData findRealSender() {
        ActorContext actorContext = InternalActorContextAccessor.getCurrent();
        if (actorContext != null) {
            RealSenderData realSenderData = new RealSenderData(
                    safeToString(actorContext.getSelf()),
                    actorContext.getSelfType());
            return realSenderData.isEmpty() ? null : realSenderData;
        }
        return ExternalRealSenderDataHolder.currentExternalSender();
    }

    @Nullable
    private static String safeToString(ActorRef actorRef) {
        return actorRef != null ? actorRef.toString() : null;
    }
}
