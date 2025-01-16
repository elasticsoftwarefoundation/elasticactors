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

package org.elasticsoftware.elasticactors.cluster.tasks;

import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.PersistentSubscription;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.logging.LoggingSettings;
import org.elasticsoftware.elasticactors.cluster.logging.MessageLogger;
import org.elasticsoftware.elasticactors.cluster.metrics.Measurement;
import org.elasticsoftware.elasticactors.cluster.metrics.MetricsSettings;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.util.concurrent.MessageHandlingThreadBoundRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;
import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;
import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * @author Joost van de Wijgerd
 */
public final class HandleUndeliverableServiceMessageTask
    implements MessageHandlingThreadBoundRunnable<String>, ActorContext {

    private static final Logger logger = LoggerFactory.getLogger(HandleUndeliverableServiceMessageTask.class);

    private final ActorRef serviceRef;
    private final InternalActorSystem actorSystem;
    private final ElasticActor serviceActor;
    private final InternalMessage internalMessage;
    private final MessageHandlerEventListener messageHandlerEventListener;
    private final Measurement measurement;
    private final MetricsSettings metricsSettings;
    private final LoggingSettings loggingSettings;
    private Class<?> unwrappedMessageClass;

    public HandleUndeliverableServiceMessageTask(
        InternalActorSystem actorSystem,
        ActorRef serviceRef,
        ElasticActor serviceActor,
        InternalMessage internalMessage,
        MessageHandlerEventListener messageHandlerEventListener,
        MetricsSettings metricsSettings,
        LoggingSettings loggingSettings)
    {
        this.serviceRef = serviceRef;
        this.actorSystem = actorSystem;
        this.serviceActor = serviceActor;
        this.internalMessage = internalMessage;
        this.messageHandlerEventListener = messageHandlerEventListener;
        this.metricsSettings = metricsSettings != null ? metricsSettings : MetricsSettings.DISABLED;
        this.loggingSettings = loggingSettings != null ? loggingSettings : LoggingSettings.DISABLED;
        this.measurement = isMeasurementEnabled() ? new Measurement(System.nanoTime()) : null;
    }

    private boolean isMeasurementEnabled() {
        return MessageLogger.isMeasurementEnabled(
            internalMessage,
            metricsSettings,
            loggingSettings,
            this::unwrapMessageClass
        );
    }

    @Override
    public ActorRef getSelf() {
        return serviceRef;
    }

    @Nullable
    @Override
    public Class<?> getSelfType() {
        return serviceActor != null ? serviceActor.getClass() : null;
    }

    @Override
    public <T extends ActorState> T getState(Class<T> stateClass) {
        return null;
    }

    @Override
    public void setState(ActorState state) {
        // state not supported
    }

    @Override
    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    @Override
    public String getKey() {
        return serviceRef.getActorId();
    }

    @Override
    public Collection<PersistentSubscription> getSubscriptions() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, Set<ActorRef>> getSubscribers() {
        return Collections.emptyMap();
    }

    @Override
    public void run() {
        try (MessagingScope ignored = getManager().enter(this, internalMessage)) {
            runInContext();
        }
    }

    private void runInContext() {
        checkDeliveryThresholdExceeded();
        Exception executionException = null;
        InternalActorContext.setContext(this);
        try {
            logMessageBasicInformation();
            Object message = deserializeMessage(actorSystem, internalMessage);
            logMessageContents(message);
            serviceActor.onUndeliverable(internalMessage.getSender(), message);
        } catch (Exception e) {
            // @todo: send an error message to the sender
            logger.error("Exception while handling message for service [{}]", serviceRef, e);
            executionException = e;
        } finally {
            InternalActorContext.clearContext();
        }
        if(messageHandlerEventListener != null) {
            if(executionException == null) {
                messageHandlerEventListener.onDone(internalMessage);
            } else {
                messageHandlerEventListener.onError(internalMessage,executionException);
            }
        }
        if (this.measurement != null) {
            logMessageTimingInformationForTraces();
            logMessageTimingInformation();
            checkMessageHandlingThresholdExceeded();
        }
    }

    @Nullable
    private Class unwrapMessageClass(InternalMessage internalMessage) {
        try {
            if (unwrappedMessageClass == null) {
                unwrappedMessageClass = getClassHelper().forName(internalMessage.getPayloadClass());
            }
            return unwrappedMessageClass;
        } catch (ClassNotFoundException e) {
            logger.error("Class [{}] not found", internalMessage.getPayloadClass());
            return null;
        }
    }

    private void logMessageBasicInformation() {
        MessageLogger.logMessageBasicInformation(
            internalMessage,
            loggingSettings,
            serviceActor,
            serviceRef,
            this::unwrapMessageClass
        );
    }

    private void logMessageContents(Object message) {
        MessageLogger.logMessageContents(
            internalMessage,
            actorSystem,
            message,
            loggingSettings,
            serviceActor,
            serviceRef,
            this::unwrapMessageClass
        );
    }

    private void logMessageTimingInformation() {
        MessageLogger.logMessageTimingInformation(
            internalMessage,
            loggingSettings,
            measurement,
            serviceActor,
            serviceRef,
            this::unwrapMessageClass
        );
    }

    private void logMessageTimingInformationForTraces() {
        MessageLogger.logMessageTimingInformationForTraces(
            this.getClass(),
            internalMessage,
            measurement,
            serviceActor,
            serviceRef
        );
    }

    private void checkMessageHandlingThresholdExceeded() {
        MessageLogger.checkMessageHandlingThresholdExceeded(
            this.getClass(),
            internalMessage,
            actorSystem,
            metricsSettings,
            measurement,
            serviceActor,
            serviceRef,
            this::unwrapMessageClass
        );
    }

    private void checkDeliveryThresholdExceeded() {
        MessageLogger.checkDeliveryThresholdExceeded(
            this.getClass(),
            internalMessage,
            actorSystem,
            metricsSettings,
            serviceActor,
            serviceRef,
            this::unwrapMessageClass
        );
    }

    @Override
    public Class<? extends ElasticActor> getActorType() {
        return serviceActor.getClass();
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
