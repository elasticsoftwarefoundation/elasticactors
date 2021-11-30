package org.elasticsoftware.elasticactors.util.concurrent;

import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;

public interface MessageHandlingThreadBoundRunnable<T> extends ThreadBoundRunnable<T> {

    Class<? extends ElasticActor> getActorType();

    Class<?> getMessageClass();

    InternalMessage getInternalMessage();
}
