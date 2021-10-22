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

package org.elasticsoftware.elasticactors.cluster.tasks.reactivestreams;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.PersistentSubscription;
import org.elasticsoftware.elasticactors.PublisherNotFoundException;
import org.elasticsoftware.elasticactors.SubscriberContext;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.tasks.ActorLifecycleTask;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.CancelMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.CompletedMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.NextMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.RequestMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.SubscribeMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.SubscriptionMessage;
import org.elasticsoftware.elasticactors.reactivestreams.InternalPersistentSubscription;
import org.elasticsoftware.elasticactors.state.MessageSubscriber;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * Task that is responsible for internalMessage deserialization, error handling and state updates
 *
 * @author Joost van de Wijged
 */
public final class HandleUndeliverableMessageTask extends ActorLifecycleTask implements SubscriberContext {
    private static final Logger log = LoggerFactory.getLogger(HandleUndeliverableMessageTask.class);
    private InternalPersistentSubscription currentSubscription;

    public HandleUndeliverableMessageTask(InternalActorSystem actorSystem,
                                          ElasticActor receiver,
                                          ActorRef receiverRef,
                                          InternalMessage internalMessage,
                                          PersistentActor persistentActor,
                                          PersistentActorRepository persistentActorRepository,
                                          MessageHandlerEventListener messageHandlerEventListener) {
        super(null, persistentActorRepository, persistentActor, actorSystem, receiver, receiverRef, messageHandlerEventListener, internalMessage, null);
    }

    // SubscriberContext implementation

    @Override
    public ActorRef getSelf() {
        return receiverRef;
    }

    @Override
    public ActorRef getPublisher() {
        return internalMessage.getSender();
    }

    @Override
    public <T extends ActorState> T getState(Class<T> stateClass) {
        return stateClass.cast(persistentActor.getState());
    }

    @Override
    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    @Override
    public PersistentSubscription getSubscription() {
        return currentSubscription;
    }

    @Override
    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        try {
            Object message = deserializeMessage(actorSystem, internalMessage);
            if(message instanceof NextMessage) {
                NextMessage nextMessage = (NextMessage) message;
                // the subscriber is gone, need to remove the subscriber reference
                return persistentActor.removeSubscriber(nextMessage.getMessageName(), new MessageSubscriber(internalMessage.getSender()));
            } else if(message instanceof CompletedMessage) {
                // the subscriber was already gone, we can ignore this
                return false;
            } else if(message instanceof CancelMessage) {
                CancelMessage cancelMessage = (CancelMessage) message;
                // was trying to cancel my subscription, publisher was gone but I need to remove local state
                return persistentActor.removeSubscription(cancelMessage.getMessageName(), internalMessage.getSender());
            } else if(message instanceof RequestMessage) {
                RequestMessage requestMessage = (RequestMessage) message;
                // tried to request data, but the publisher doesn't exist (anymore)
                // calling cancel will cause a cancel message to be sent to the publishing actor, which will result
                // in another undeliverable message but the internal state of the PersistentSubscription will be correct
                persistentActor.cancelSubscription(requestMessage.getMessageName(), internalMessage.getSender());
                return persistentActor.removeSubscription(requestMessage.getMessageName(), internalMessage.getSender());
            } else if(message instanceof SubscribeMessage) {
                // subscribe failed, the publishing actor doesn't exist
                // signal onError or delegate to special undeliverable handler if present
                SubscribeMessage subscribeMessage = (SubscribeMessage) message;
                Optional<InternalPersistentSubscription> persistentSubscription =
                    persistentActor.getSubscription(subscribeMessage.getMessageName(), internalMessage.getSender());
                if(persistentSubscription.isPresent()) {
                    // need to remove it from the internal state before we handle the error
                    persistentActor.removeSubscription(subscribeMessage.getMessageName(), internalMessage.getSender());
                    currentSubscription = persistentSubscription.get();
                    InternalSubscriberContext.setContext(this);
                    try {
                        currentSubscription.getSubscriber().onError(new PublisherNotFoundException(
                                String.format("Actor[%s] does not exist", internalMessage.getSender()),
                                internalMessage.getSender()));
                    } catch(Exception e) {
                        log.error("Unexpected Exception while calling onError on Subscriber with type {} of Actor {}",
                                currentSubscription.getSubscriber() != null ?
                                        currentSubscription.getSubscriber().getClass().getSimpleName() : null,
                                receiverRef, e);
                    } finally {
                        InternalSubscriberContext.clearContext();
                    }
                    return true;
                } else {
                    return false;
                }
            } else if(message instanceof SubscriptionMessage) {
                SubscriptionMessage subscriptionMessage = (SubscriptionMessage) message;
                // the subscriber is gone, need to remove the subscriber reference
                return persistentActor.removeSubscriber(subscriptionMessage.getMessageName(), new MessageSubscriber(internalMessage.getSender()));
            }
        } catch (Exception e) {
            log.error("Exception while Deserializing Message class {} in ActorSystem [{}]",
                    internalMessage.getPayloadClass(), actorSystem.getName(), e);
        }
        return false;
    }
}
