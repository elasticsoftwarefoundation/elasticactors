/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.cluster.tasks;

import org.elasticsoftware.elasticactors.ActorLifecycleListener;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.ShardKey;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.UUIDTools;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;
import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.PersistenceAdvisor;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;
import org.elasticsoftware.elasticactors.state.PersistenceConfigHelper;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.elasticsoftware.elasticactors.tracing.MessagingContextManager.MessagingScope;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsoftware.elasticactors.tracing.MessagingContextManager.getManager;
import static org.elasticsoftware.elasticactors.tracing.TracingUtils.shorten;
import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * @author Joost van de Wijgerd
 */
public abstract class ActorLifecycleTask implements ThreadBoundRunnable<String> {
    private static final Logger log = LoggerFactory.getLogger(ActorLifecycleTask.class);
    protected final ActorRef receiverRef;
    protected final ElasticActor receiver;
    protected final InternalActorSystem actorSystem;
    protected final PersistentActor persistentActor;
    protected final PersistentActorRepository persistentActorRepository;
    protected final InternalMessage internalMessage;
    private final MessageHandlerEventListener messageHandlerEventListener;
    private final Measurement measurement;
    private final ActorStateUpdateProcessor actorStateUpdateProcessor;

    protected final static boolean metricsEnabled =
        SystemPropertiesResolver.getProperty("ea.metrics.messaging.enabled", Boolean.class, false);
    private final static Long messageDeliveryWarnThreshold =
        SystemPropertiesResolver.getProperty("ea.metrics.messaging.delivery.warn.threshold", Long.class);
    private final static Long messageHandlingWarnThreshold =
        SystemPropertiesResolver.getProperty("ea.metrics.messaging.handling.warn.threshold", Long.class);
    private final static Long serializationWarnThreshold =
        SystemPropertiesResolver.getProperty("ea.serialization.warn.threshold", Long.class);

    protected ActorLifecycleTask(
        ActorStateUpdateProcessor actorStateUpdateProcessor,
        PersistentActorRepository persistentActorRepository,
        PersistentActor persistentActor,
        InternalActorSystem actorSystem,
        ElasticActor receiver,
        ActorRef receiverRef,
        MessageHandlerEventListener messageHandlerEventListener,
        InternalMessage internalMessage)
    {
        this.actorStateUpdateProcessor = actorStateUpdateProcessor;
        this.persistentActorRepository = persistentActorRepository;
        this.receiverRef = receiverRef;
        this.persistentActor = persistentActor;
        this.actorSystem = actorSystem;
        this.receiver = receiver;
        this.messageHandlerEventListener = messageHandlerEventListener;
        this.internalMessage = internalMessage;
        this.measurement = isMeasurementEnabled() ? new Measurement(System.nanoTime()) : null;
    }

    protected boolean isMeasurementEnabled() {
        boolean enabledForMetrics = metricsEnabled
            && (messageDeliveryWarnThreshold != null || messageHandlingWarnThreshold != null);
        return enabledForMetrics
            || serializationWarnThreshold != null
            || log.isTraceEnabled();
    }

    @Override
    public final String getKey() {
        return (persistentActor.getAffinityKey() != null) ? persistentActor.getAffinityKey() : receiverRef.getActorId();
    }

    @Override
    public final void run() {
        try (MessagingScope ignored = getManager().enter(persistentActor, internalMessage)) {
            runInContext();
        }
    }

    private void runInContext() {
        checkDeliveryThresholdExceeded();
        // measure start of the execution
        if(this.measurement != null) {
            this.measurement.setExecutionStart(System.nanoTime());
        }
        // setup the context
        Exception executionException = null;
        InternalActorContext.setContext(persistentActor);
        SerializationContext.initialize();
        boolean shouldUpdateState = false;
        try {
            shouldUpdateState = doInActorContext(actorSystem, receiver, receiverRef, internalMessage);
            executeLifecycleListeners();
        } catch (Exception e) {
            log.error("Exception in doInActorContext",e);
            executionException = e;
        } finally {
            // reset the serialization context
            SerializationContext.reset();
            // clear the state from the thread
            InternalActorContext.clearContext();
            // marks the end of the execution path
            if(this.measurement != null) {
                this.measurement.setExecutionEnd(System.nanoTime());
            }
            // check if we have state now that needs to be written to the persistent actor store
            if (persistentActorRepository != null && persistentActor.getState() != null && shouldUpdateState) {
                try {
                    // generate the serialized state
                    persistentActor.serializeState();
                    persistentActorRepository.updateAsync((ShardKey) persistentActor.getKey(), persistentActor,
                                                          internalMessage, messageHandlerEventListener);
                    // if we have a configured actor state update processor, then use it
                    if(actorStateUpdateProcessor != null) {
                        // this is either a lifecycle step or an incoming message
                        if(getLifeCycleStep() != null) {
                            actorStateUpdateProcessor.process(getLifeCycleStep(), null, persistentActor);
                        } else {
                            // it's an incoming message so the messageClass in the internal message is what we need
                            // to support multiple internal message handling protocols (such as the reactive streams protocol)
                            // we need to unwrap here to find the actual message
                            Class messageClass = unwrapMessageClass(internalMessage);
                            if (messageClass != null) {
                                actorStateUpdateProcessor.process(
                                    null,
                                    messageClass,
                                    persistentActor
                                );
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception while serializing ActorState for actor [{}]", receiverRef.getActorId(), e);
                } finally {
                    // always ensure we release the memory of the serialized state
                     persistentActor.setSerializedState(null);
                }
                // measure the serialization time
                if(this.measurement != null) {
                    this.measurement.setSerializationEnd(System.nanoTime());
                }
            } else if(messageHandlerEventListener != null) {
                if(executionException == null) {
                    messageHandlerEventListener.onDone(internalMessage);
                } else {
                    messageHandlerEventListener.onError(internalMessage,executionException);
                }
                // measure the ack time
                if(this.measurement != null) {
                    this.measurement.setAckEnd(System.nanoTime());
                }
            }
            // do some trace logging
            if(this.measurement != null) {
                if (log.isTraceEnabled()) {
                    log.trace(
                        "({}) Message of type [{}] with id [{}] for actor [{}] took {} microsecs "
                            + "in queue, {} microsecs to execute, {} microsecs to serialize and "
                            + "{} microsecs to ack (state update {})",
                        this.getClass().getSimpleName(),
                        (internalMessage != null) ? internalMessage.getPayloadClass() : null,
                        (internalMessage != null) ? internalMessage.getId() : null,
                        receiverRef.getActorId(),
                        measurement.getQueueDuration(MICROSECONDS),
                        measurement.getExecutionDuration(MICROSECONDS),
                        measurement.getSerializationDuration(MICROSECONDS),
                        measurement.getAckDuration(MICROSECONDS),
                        measurement.isSerialized()
                    );
                }

                if (serializationWarnThreshold != null && this.measurement.getSerializationDuration(TimeUnit.MICROSECONDS) > serializationWarnThreshold) {
                    log.warn("({}) Message of type [{}] with id [{}] triggered serialization for actor [{}] which took {} microsecs to complete",this.getClass().getSimpleName(),(internalMessage != null) ? internalMessage.getPayloadClass() : "null",(internalMessage != null) ? internalMessage.getId() : "null",receiverRef.getActorId(),measurement.getSerializationDuration(MICROSECONDS));
                }

                if (metricsEnabled) {
                    if (messageHandlingWarnThreshold != null) {

                    }
                }
            }
        }
    }

    protected abstract boolean doInActorContext(InternalActorSystem actorSystem,
                                                ElasticActor receiver,
                                                ActorRef receiverRef,
                                                InternalMessage internalMessage);

    private void executeLifecycleListeners() {
        final List<ActorLifecycleListener<?>> lifecycleListeners = actorSystem.getActorLifecycleListeners(persistentActor.getActorClass());
        if(lifecycleListeners != null) {
            for (ActorLifecycleListener<?> lifecycleListener : lifecycleListeners) {
                try {
                    executeLifecycleListener(lifecycleListener,persistentActor.getSelf(),persistentActor.getState());
                } catch(Throwable t) {
                    log.warn("Exception while executing ActorLifecycleListener",t);
                }
            }
        }
    }

    protected ActorLifecycleStep executeLifecycleListener(ActorLifecycleListener listener,ActorRef actorRef,ActorState actorState) {
        return null;
    }

    protected ActorLifecycleStep getLifeCycleStep() {
        return null;
    }

    public static boolean shouldUpdateState(ElasticActor elasticActor,ActorLifecycleStep lifecycleStep) {
        if (elasticActor instanceof PersistenceAdvisor) {
            return ((PersistenceAdvisor)elasticActor).shouldUpdateState(lifecycleStep);
        } else {
            PersistenceConfig persistenceConfig = elasticActor.getClass().getAnnotation(PersistenceConfig.class);
            return PersistenceConfigHelper.shouldUpdateState(persistenceConfig,lifecycleStep);
        }
    }

    public static boolean shouldUpdateState(ElasticActor elasticActor, Object message) {
        if (elasticActor instanceof PersistenceAdvisor) {
            return ((PersistenceAdvisor)elasticActor).shouldUpdateState(message);
        } else {
            // look at the annotation on the actor class
            PersistenceConfig persistenceConfig = elasticActor.getClass().getAnnotation(PersistenceConfig.class);
            return PersistenceConfigHelper.shouldUpdateState(persistenceConfig,message);
        }
    }

    /**
     * Return the actual message class that is being handled. By default, this method will return
     * {@link InternalMessage#getPayloadClass()} but it can be overridden to support other protocols
     * that wrap the ultimate message being delivered.
     *
     * @param internalMessage the internal representation of this message
     * @return the actual message class that is being handled, null if it cannot be resolved
     */
    @Nullable
    protected Class unwrapMessageClass(InternalMessage internalMessage) {
        try {
            return getClassHelper().forName(internalMessage.getPayloadClass());
        } catch (ClassNotFoundException e) {
            log.error("Class [{}] not found", internalMessage.getPayloadClass());
            return null;
        }
    }

    private void checkDeliveryThresholdExceeded() {
        if (internalMessage != null
            && metricsEnabled
            && messageDeliveryWarnThreshold != null
            && log.isWarnEnabled())
        {
            long timestamp = UUIDTools.toUnixTimestamp(internalMessage.getId());
            long delay = timestamp - System.currentTimeMillis();
            if (delay > messageDeliveryWarnThreshold) {
                log.warn(
                    "Message delivery delay of {} ms exceeds the threshold of {} ms. "
                        + "Receivers [{}]. "
                        + "Sender [{}]. "
                        + "Message type [{}]. "
                        + "Message envelope type [{}]. "
                        + "Trace context: [{}]"
                        + "Creation context: [{}]"
                        + "Message payload size (in bytes): {}",
                    delay,
                    messageDeliveryWarnThreshold,
                    internalMessage.getReceivers(),
                    internalMessage.getSender(),
                    shorten(internalMessage.getPayloadClass()),
                    shorten(internalMessage.getClass()),
                    internalMessage.getTraceContext(),
                    internalMessage.getCreationContext(),
                    internalMessage.hasSerializedPayload()
                        ? internalMessage.getPayload().limit()
                        : "N/A"
                );
            }
        }
    }
}
