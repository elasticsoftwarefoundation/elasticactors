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
import org.elasticsoftware.elasticactors.tracing.Tracer;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * @author Joost van de Wijgerd
 */
public final class HandleUndeliverableServiceMessageTask implements ThreadBoundRunnable<String>, ActorContext {
    private static final Logger logger = LogManager.getLogger(HandleUndeliverableServiceMessageTask.class);
    private final ActorRef serviceRef;
    private final InternalActorSystem actorSystem;
    private final ElasticActor serviceActor;
    private final InternalMessage internalMessage;
    private final MessageHandlerEventListener messageHandlerEventListener;

    public HandleUndeliverableServiceMessageTask(InternalActorSystem actorSystem,
                                                 ActorRef serviceRef,
                                                 ElasticActor serviceActor,
                                                 InternalMessage internalMessage,
                                                 MessageHandlerEventListener messageHandlerEventListener) {
        this.serviceRef = serviceRef;
        this.actorSystem = actorSystem;
        this.serviceActor = serviceActor;
        this.internalMessage = internalMessage;
        this.messageHandlerEventListener = messageHandlerEventListener;
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
        Exception executionException = null;
        InternalActorContext.setContext(this);
        try {
            Tracer.get().throwingRunInCurrentTrace(this::internalHandleUndeliverable);
        } catch(Exception e) {
            // @todo: send an error message to the sender
            logger.error(String.format("Exception while handling message for service [%s]",serviceRef.toString()),e);
            executionException = e;
        } finally {
            InternalActorContext.getAndClearContext();
        }
        if(messageHandlerEventListener != null) {
            if(executionException == null) {
                messageHandlerEventListener.onDone(internalMessage);
            } else {
                messageHandlerEventListener.onError(internalMessage,executionException);
            }
        }
    }

    private void internalHandleUndeliverable() throws Exception {
        Object message = deserializeMessage(actorSystem, internalMessage);
        serviceActor.onUndeliverable(internalMessage.getSender(), message);
    }
}
