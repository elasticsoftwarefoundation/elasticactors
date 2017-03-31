/*
 * Copyright 2013 - 2017 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsoftware.elasticactors.cluster.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;
import org.elasticsoftware.elasticactors.state.*;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * @author Joost van de Wijgerd
 */
public abstract class ActorLifecycleTask implements ThreadBoundRunnable<String> {
    private static final Logger log = LogManager.getLogger(ActorLifecycleTask.class);
    protected final ActorRef receiverRef;
    protected final ElasticActor receiver;
    protected final InternalActorSystem actorSystem;
    protected final PersistentActor persistentActor;
    protected final PersistentActorRepository persistentActorRepository;
    protected final InternalMessage internalMessage;
    private final MessageHandlerEventListener messageHandlerEventListener;
    private final Measurement measurement;
    private final ActorStateUpdateProcessor actorStateUpdateProcessor;

    protected ActorLifecycleTask(ActorStateUpdateProcessor actorStateUpdateProcessor,
                                 PersistentActorRepository persistentActorRepository,
                                 PersistentActor persistentActor,
                                 InternalActorSystem actorSystem,
                                 ElasticActor receiver,
                                 ActorRef receiverRef,
                                 MessageHandlerEventListener messageHandlerEventListener,
                                 InternalMessage internalMessage) {
        this.actorStateUpdateProcessor = actorStateUpdateProcessor;
        this.persistentActorRepository = persistentActorRepository;
        this.receiverRef = receiverRef;
        this.persistentActor = persistentActor;
        this.actorSystem = actorSystem;
        this.receiver = receiver;
        this.messageHandlerEventListener = messageHandlerEventListener;
        this.internalMessage = internalMessage;
        // only measure when trace is enabled
        this.measurement = log.isTraceEnabled() ? new Measurement(System.nanoTime()) : null;
    }

    @Override
    public final String getKey() {
        return (persistentActor.getAffinityKey() != null) ? persistentActor.getAffinityKey() : receiverRef.getActorId();
    }

    @Override
    public final void run() {
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
            InternalActorContext.getAndClearContext();
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
                            unwrapMessageClass(internalMessage).ifPresent(messageClass ->
                                    actorStateUpdateProcessor.process(null, messageClass, persistentActor));
                        }
                    }
                } catch (Exception e) {
                    log.error(format("Exception while serializing ActorState for actor [%s]", receiverRef.getActorId()), e);
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
                log.trace(format("(%s) Message of type [%s] with id [%s] for actor [%s] took %d microsecs in queue, %d microsecs to execute, %d microsecs to serialize and %d microsecs to ack (state update %b)",this.getClass().getSimpleName(),(internalMessage != null) ? internalMessage.getPayloadClass() : "null",(internalMessage != null) ? internalMessage.getId().toString() : "null",receiverRef.getActorId(),measurement.getQueueDuration(MICROSECONDS),measurement.getExecutionDuration(MICROSECONDS),measurement.getSerializationDuration(MICROSECONDS),measurement.getAckDuration(MICROSECONDS),measurement.isSerialized()));
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

    protected final boolean shouldUpdateState(ElasticActor elasticActor,ActorLifecycleStep lifecycleStep) {
        if(PersistenceAdvisor.class.isAssignableFrom(elasticActor.getClass())) {
            return ((PersistenceAdvisor)elasticActor).shouldUpdateState(lifecycleStep);
        } else {
            PersistenceConfig persistenceConfig = elasticActor.getClass().getAnnotation(PersistenceConfig.class);
            return persistenceConfig == null || Arrays.asList(persistenceConfig.persistOn()).contains(lifecycleStep);
        }
    }

    protected final boolean shouldUpdateState(ElasticActor elasticActor, Object message) {
        if(PersistenceAdvisor.class.isAssignableFrom(elasticActor.getClass())) {
            return ((PersistenceAdvisor)elasticActor).shouldUpdateState(message);
        } else {
            // look at the annotation on the actor class
            PersistenceConfig persistenceConfig = elasticActor.getClass().getAnnotation(PersistenceConfig.class);
            if (persistenceConfig != null) {
                // look for not excluded when persist all is on
                if(persistenceConfig.persistOnMessages()) {
                    return !Arrays.asList(persistenceConfig.excluded()).contains(message.getClass());
                } else {
                    // look for included otherwise
                    return Arrays.asList(persistenceConfig.included()).contains(message.getClass());
                }
            } else {
                return true;
            }
        }
    }

    /**
     * Return the actual message class that is being handled. By default this method will return
     * {@link InternalMessage#getPayloadClass()} but it can be overridden to support other protocols that wrap the
     * ultimate message being delivered. If the
     *
     * @param internalMessage
     * @return
     * @throws ClassNotFoundException
     */
    protected Optional<Class> unwrapMessageClass(InternalMessage internalMessage) {
        try {
            return Optional.of(Class.forName(internalMessage.getPayloadClass()));
        } catch (ClassNotFoundException e) {
            return Optional.empty();
        }
    }
}
