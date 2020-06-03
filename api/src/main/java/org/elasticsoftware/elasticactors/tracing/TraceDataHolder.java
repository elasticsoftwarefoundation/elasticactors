package org.elasticsoftware.elasticactors.tracing;

public class TraceDataHolder {

    protected static final ThreadLocal<TraceData> threadTraceData = new ThreadLocal<>();

    protected TraceDataHolder() {

    }

    public static TraceData currentTraceData() {
        return threadTraceData.get();
    }

}
