package org.elasticsoftware.elasticactors.tracing;

public class ExternalRealSenderDataHolder {

    protected static final ThreadLocal<RealSenderData> threadRealSender = new ThreadLocal<>();

    protected ExternalRealSenderDataHolder() {

    }

    public static RealSenderData currentExternalSender() {
        return threadRealSender.get();
    }

}
