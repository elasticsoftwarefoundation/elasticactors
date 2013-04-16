/*
 * Copyright 2013 Joost van de Wijgerd
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

package org.elasterix.elasticactors.cluster.tasks;

import org.apache.log4j.Logger;
import org.elasterix.elasticactors.*;
import org.elasterix.elasticactors.cluster.InternalActorSystem;
import org.elasterix.elasticactors.messaging.InternalMessage;
import org.elasterix.elasticactors.messaging.MessageHandlerEventListener;
import org.elasterix.elasticactors.util.concurrent.ThreadBoundRunnable;

import static org.elasterix.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * @author Joost van de Wijgerd
 */
public class HandleServiceMessageTask implements ThreadBoundRunnable<String>, ActorContext {
    private static final Logger logger = Logger.getLogger(HandleServiceMessageTask.class);
    private final ActorRef serviceRef;
    private final InternalActorSystem actorSystem;
    private final ElasticActor serviceActor;
    private final InternalMessage internalMessage;
    private final MessageHandlerEventListener messageHandlerEventListener;

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
    }

    @Override
    public ActorRef getSelf() {
        return serviceRef;
    }

    @Override
    public ActorState getState() {
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
    public void run() {
        Exception executionException = null;
        InternalActorContext.setContext(this);
        try {
            Object message = deserializeMessage(actorSystem, internalMessage);
            serviceActor.onReceive(message,internalMessage.getSender());
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
}
