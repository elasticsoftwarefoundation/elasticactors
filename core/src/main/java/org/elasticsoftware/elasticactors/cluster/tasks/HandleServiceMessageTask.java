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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.PersistentSubscription;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * @author Joost van de Wijgerd
 */
public final class HandleServiceMessageTask implements ThreadBoundRunnable<String>, ActorContext {
    private static final Logger logger = LogManager.getLogger(HandleServiceMessageTask.class);
    private final ActorRef serviceRef;
    private final InternalActorSystem actorSystem;
    private final ElasticActor serviceActor;
    private final InternalMessage internalMessage;
    private final MessageHandlerEventListener messageHandlerEventListener;
    private final Measurement measurement;

    public HandleServiceMessageTask(InternalActorSystem actorSystem,
                                    ActorRef serviceRef,
                                    ElasticActor serviceActor,
                                    InternalMessage internalMessage,
                                    MessageHandlerEventListener messageHandlerEventListener) {
        this.serviceRef = serviceRef;
        this.actorSystem = actorSystem;
        this.serviceActor = serviceActor;
        this.internalMessage = internalMessage;
        this.messageHandlerEventListener = messageHandlerEventListener;
        // only measure when trace is enabled
        this.measurement = logger.isTraceEnabled() ? new Measurement(System.nanoTime()) : null;
    }

    @Override
    public ActorRef getSelf() {
        return serviceRef;
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
    public final void run() {
        // measure start of the execution
        if(this.measurement != null) {
            this.measurement.setExecutionStart(System.nanoTime());
        }
        Exception executionException = null;
        InternalActorContext.setContext(this);
        try {
            Object message = deserializeMessage(actorSystem, internalMessage);
            serviceActor.onReceive(internalMessage.getSender(), message);
        } catch(Exception e) {
            // @todo: send an error message to the sender
            logger.error(String.format("Exception while handling message for service [%s]",serviceRef.toString()),e);
            executionException = e;
        } finally {
            InternalActorContext.getAndClearContext();
        }
        // marks the end of the execution path
        if(this.measurement != null) {
            this.measurement.setExecutionEnd(System.nanoTime());
        }
        if(messageHandlerEventListener != null) {
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
            logger.trace(format("(%s) Message of type [%s] with id [%s] for actor [%s] took %d microsecs in queue, %d microsecs to execute, 0 microsecs to serialize and %d microsecs to ack (state update false)",this.getClass().getSimpleName(),(internalMessage != null) ? internalMessage.getPayloadClass() : "null",(internalMessage != null) ? internalMessage.getId().toString() : "null",serviceRef.getActorId(),measurement.getQueueDuration(MICROSECONDS),measurement.getExecutionDuration(MICROSECONDS),measurement.getAckDuration(MICROSECONDS)));
        }
    }
}
