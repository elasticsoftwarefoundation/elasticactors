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

package org.elasticsoftware.elasticactors.cluster.tasks.reactivestreams;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.tasks.ActorLifecycleTask;
import org.elasticsoftware.elasticactors.cluster.tasks.ProtocolFactory;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;

/**
 * @author Joost van de Wijgerd
 */
public final class ReactiveStreamsProtocolFactory implements ProtocolFactory {
    @Override
    public ActorLifecycleTask createHandleMessageTask(InternalActorSystem actorSystem,
                                                      ElasticActor receiver,
                                                      ActorRef receiverRef,
                                                      InternalMessage internalMessage,
                                                      PersistentActor persistentActor,
                                                      PersistentActorRepository persistentActorRepository,
                                                      MessageHandlerEventListener messageHandlerEventListener,
                                                      Boolean measure,
                                                      Long serializationWarnThreshold) {
        return new HandleMessageTask(actorSystem, receiver, receiverRef, internalMessage, persistentActor,
                persistentActorRepository, messageHandlerEventListener);
    }

    @Override
    public ActorLifecycleTask createHandleUndeliverableMessageTask(InternalActorSystem actorSystem,
                                                                   ElasticActor receiver,
                                                                   ActorRef receiverRef,
                                                                   InternalMessage internalMessage,
                                                                   PersistentActor persistentActor,
                                                                   PersistentActorRepository persistentActorRepository,
                                                                   MessageHandlerEventListener messageHandlerEventListener) {
        return new HandleUndeliverableMessageTask(actorSystem,
                                                  receiver,
                                                  receiverRef,
                                                  internalMessage,
                                                  persistentActor,
                                                  persistentActorRepository,
                                                  messageHandlerEventListener);
    }
}
