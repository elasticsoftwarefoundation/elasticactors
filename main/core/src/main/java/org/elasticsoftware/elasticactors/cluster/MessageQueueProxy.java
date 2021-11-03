package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.messaging.InternalMessage;

public interface MessageQueueProxy {

    void init() throws Exception;

    void destroy();

    void offerInternalMessage(InternalMessage message);
}
