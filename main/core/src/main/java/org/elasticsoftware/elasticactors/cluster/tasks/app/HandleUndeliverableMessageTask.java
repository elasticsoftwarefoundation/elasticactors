/*
 * Copyright 2013 - 2025 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.cluster.tasks.app;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.MessageDeliveryException;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.logging.LoggingSettings;
import org.elasticsoftware.elasticactors.cluster.metrics.MetricsSettings;
import org.elasticsoftware.elasticactors.cluster.tasks.ActorLifecycleTask;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.elasticsoftware.elasticactors.util.concurrent.MessageHandlingThreadBoundRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * Task that is responsible for internalMessage deserialization, error handling and state updates
 *
 * @author Joost van de Wijged
 */
final class HandleUndeliverableMessageTask
    extends ActorLifecycleTask
    implements MessageHandlingThreadBoundRunnable<String> {

    private static final Logger log = LoggerFactory.getLogger(HandleUndeliverableMessageTask.class);

    HandleUndeliverableMessageTask(
        InternalActorSystem actorSystem,
        ElasticActor receiver,
        ActorRef receiverRef,
        InternalMessage internalMessage,
        PersistentActor persistentActor,
        PersistentActorRepository persistentActorRepository,
        MessageHandlerEventListener messageHandlerEventListener,
        MetricsSettings metricsSettings,
        LoggingSettings loggingSettings)
    {
        super(
            null,
            persistentActorRepository,
            persistentActor,
            actorSystem,
            receiver,
            receiverRef,
            messageHandlerEventListener,
            internalMessage,
            metricsSettings,
            loggingSettings
        );
    }

    @Override
    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        try {
            Object message = deserializeMessage(actorSystem, internalMessage);
            logMessageContents(message);
            try {
                receiver.onUndeliverable(internalMessage.getSender(), message);
                return shouldUpdateState(receiver, message);
            } catch(MessageDeliveryException e) {
                // see if it is a recoverable exception
                if(!e.isRecoverable()) {
                    log.error("Unrecoverable MessageDeliveryException while handling message for actor [{}]", receiverRef, e);
                }
                // message cannot be sent but state should be updated as the received message did most likely change
                // the state
                return shouldUpdateState(receiver, message);
            } catch (Exception e) {
                log.error("Exception while handling undeliverable message for actor [{}]", receiverRef, e);
                return false;
            }
        } catch (Exception e) {
            log.error("Exception while Deserializing Message class {} in ActorSystem [{}]",
                    internalMessage.getPayloadClass(), actorSystem.getName(), e);
            return false;
        }
    }

    @Override
    protected boolean shouldLogMessageInformation() {
        return true;
    }

    @Override
    public Class<? extends ElasticActor> getActorType() {
        return receiver.getClass();
    }

    @Override
    public Class<?> getMessageClass() {
        return unwrapMessageClass(internalMessage);
    }

    @Override
    public InternalMessage getInternalMessage() {
        return internalMessage;
    }
}
