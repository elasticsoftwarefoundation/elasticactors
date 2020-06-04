package org.elasticsoftware.elasticactors.cluster.tracing;

import org.elasticsoftware.elasticactors.tracing.ExternalRealSenderDataHolder;
import org.elasticsoftware.elasticactors.tracing.RealSenderData;

public final class ExternalRealSenderDataContext {

    private static final class InternalRealSenderDataHolder extends ExternalRealSenderDataHolder {

        private static RealSenderData replaceExternalSender(RealSenderData sender) {
            RealSenderData current = threadRealSender.get();
            threadRealSender.set(sender);
            return current;
        }
    }

    private final static ThreadLocal<RealSenderData> previousExternalSender = new ThreadLocal<>();

    private ExternalRealSenderDataContext() {
    }

    public static void enter(RealSenderData sender) {
        previousExternalSender.set(InternalRealSenderDataHolder.replaceExternalSender(sender));
    }

    public static void leave() {
        InternalRealSenderDataHolder.replaceExternalSender(previousExternalSender.get());
        previousExternalSender.set(null);
    }

}
