package org.elasticsoftware.elasticactors.messaging;

public interface MessageQueueProxy {

    void init() throws Exception;

    void destroy();

    void offerInternalMessage(InternalMessage message);
}
