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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.tasks.ActorLifecycleTask;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.MessageHandlerEventListener;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.*;
import org.elasticsoftware.elasticactors.reactivestreams.InternalPersistentSubscription;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.MessageSubscriber;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.reactivestreams.Subscriber;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * Task that is responsible for internalMessage deserialization, error handling and state updates
 *
 * @author Joost van de Wijged
 */
public final class HandleMessageTask extends ActorLifecycleTask implements SubscriberContext {
    private static final Logger log = LogManager.getLogger(HandleMessageTask.class);
    private InternalPersistentSubscription currentSubscription;

    HandleMessageTask(InternalActorSystem actorSystem,
                      ElasticActor receiver,
                      ActorRef receiverRef,
                      InternalMessage internalMessage,
                      PersistentActor persistentActor,
                      PersistentActorRepository persistentActorRepository,
                      ActorStateUpdateProcessor actorStateUpdateProcessor,
                      MessageHandlerEventListener messageHandlerEventListener,
                      Long serializationWarningThreshold) {
        super(actorStateUpdateProcessor,persistentActorRepository, persistentActor, actorSystem, receiver, receiverRef, messageHandlerEventListener, internalMessage, serializationWarningThreshold);
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
    protected Optional<Class> unwrapMessageClass(InternalMessage internalMessage)  {
        if(NextMessage.class.getName().equals(internalMessage.getPayloadClass())) {
            try {
                NextMessage nextMessage = (NextMessage) internalMessage.getPayload(
                        actorSystem.getDeserializer(Class.forName(internalMessage.getPayloadClass())));
                return Optional.of(Class.forName(nextMessage.getMessageName()));
            } catch(IOException | ClassNotFoundException e) {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }

    }

    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        try {
            Object message = deserializeMessage(actorSystem, internalMessage);
            if(message instanceof NextMessage) {
                return handle((NextMessage) message, receiver, internalMessage.getSender(), actorSystem);
            } else if(message instanceof SubscribeMessage) {
                handle((SubscribeMessage) message, ((SubscribeMessage) message).getSubscriberRef());
            } else if(message instanceof CancelMessage) {
                handle((CancelMessage) message, ((CancelMessage) message).getSubscriberRef());
            } else if(message instanceof RequestMessage) {
                handle((RequestMessage) message, internalMessage.getSender());
            } else if(message instanceof SubscriptionMessage) {
                handle((SubscriptionMessage) message, receiverRef, internalMessage.getSender());
            } else if(message instanceof CompletedMessage) {
                handle((CompletedMessage) message, internalMessage.getSender());
            }
            return true;
        } catch (Exception e) {
            log.error(format("Exception while Deserializing Message class %s in ActorSystem [%s]",
                    internalMessage.getPayloadClass(), actorSystem.getName()), e);
            return false;
        }
    }

    private boolean handle(NextMessage nextMessage, ElasticActor receiver, ActorRef publisherRef, InternalActorSystem actorSystem) {
        Optional<InternalPersistentSubscription> persistentSubscription =
                persistentActor.getSubscription(nextMessage.getMessageName(), publisherRef);
        currentSubscription = persistentSubscription.get();
        InternalSubscriberContext.setContext(this);

        if(persistentSubscription.isPresent()) {
            try {
                // @todo: for now the message name == messageClass
                Class<?> messageClass = Class.forName(nextMessage.getMessageName());
                MessageDeserializer<?> deserializer = actorSystem.getDeserializer(messageClass);
                Object message = SerializationContext.deserialize(deserializer, ByteBuffer.wrap(nextMessage.getMessageBytes()));

                currentSubscription.getSubscriber().onNext(message);

                return shouldUpdateState(receiver, message);
            } catch (ClassNotFoundException e) {
                // the message type (class) that I am subscribing to is not available
                log.error(format("Actor[%s]: Could not find message type: <%s>, unable to deserialize subscribed message", receiverRef.toString(), nextMessage.getMessageName()));
            } catch (IOException e) {
                log.error(format("Actor[%s]: Problem trying to deserialize message embedded in NextMessage", receiverRef.toString()), e);
            } catch (Exception e) {
                log.error(format("Unexpected Exception while calling onNext on Subscriber with type %s of Actor %s",
                        currentSubscription.getSubscriber() != null ?
                                currentSubscription.getSubscriber().getClass().getSimpleName() : null,
                        receiverRef), e);
            } finally {
                InternalSubscriberContext.getAndClearContext();
            }
        } else {
            // there is no corresponding persistent subscription related to this publisher/message combination
            // this should not happen, however if it does happen we need to cancel the subscriber on the publisher side
            log.error(format("Subscriber %s is missing PersistentSubscription for Publisher %s while handling NextMessage", receiverRef, publisherRef));
            publisherRef.tell(new CancelMessage(receiverRef, nextMessage.getMessageName()));

        }
        return false;
    }

    private void handle(SubscribeMessage subscribeMessage, ActorRef subscriberRef) {
        persistentActor.addSubscriber(subscribeMessage.getMessageName(), new MessageSubscriber(subscriberRef));
        // let the subscriber know he has a subscription
        subscriberRef.tell(new SubscriptionMessage(subscribeMessage.getMessageName()), receiverRef);
    }

    private void handle(CancelMessage cancelMessage, ActorRef subscriberRef) {
        if(persistentActor.removeSubscriber(cancelMessage.getMessageName(), new MessageSubscriber(subscriberRef))) {
            // let the subscriber know his subscription was cancelled (completed)
            subscriberRef.tell(new CompletedMessage(cancelMessage.getMessageName()));
        }
    }

    private void handle(RequestMessage requestMessage, ActorRef subscriberRef) {
        if(persistentActor.getMessageSubscribers() != null) {
            ((Set<MessageSubscriber>)persistentActor.getMessageSubscribers().get(requestMessage.getMessageName()))
                    .stream().filter(m -> m.getSubscriberRef().equals(subscriberRef)).findFirst()
                    .ifPresent(messageSubscriber -> messageSubscriber.incrementAndGet(requestMessage.getN()));
        }
    }

    private void handle(SubscriptionMessage subscriptionMessage, ActorRef subscriberRef, ActorRef publisherRef) {
        Optional<InternalPersistentSubscription> persistentSubscription =
                persistentActor.getSubscription(subscriptionMessage.getMessageName(), publisherRef);
        if(persistentSubscription.isPresent()) {
            currentSubscription = persistentSubscription.get();
            // notify the subscriber
            InternalSubscriberContext.setContext(this);
            try {
                persistentSubscription.get().getSubscriber().onSubscribe(persistentSubscription.get());
            } catch(Exception e) {
                log.error(format("Unexpected Exception while calling onSubscribe on Subscriber with type %s of Actor %s",
                        currentSubscription.getSubscriber() != null ?
                                currentSubscription.getSubscriber().getClass().getSimpleName() : null,
                        receiverRef), e);
            } finally {
                InternalSubscriberContext.getAndClearContext();
            }
        } else {
            // we got a subscription message, but there is no corresponding PersistentSubscription ... this should not
            // be possible. however since the other side now has a corresponding subscriber reference we need to cancel
            // it to keep
            log.error(format("Subscriber %s is missing PersistentSubscription for Publisher %s while handling SubscriptionMessage", receiverRef, publisherRef));
            publisherRef.tell(new CancelMessage(subscriberRef, subscriptionMessage.getMessageName()));
        }

    }

    private void handle(CompletedMessage completedMessage, ActorRef publisherRef) {
        Optional<InternalPersistentSubscription> persistentSubscription =
                persistentActor.getSubscription(completedMessage.getMessageName(), publisherRef);
        if(persistentSubscription.isPresent()) {
            currentSubscription = persistentSubscription.get();
            InternalSubscriberContext.setContext(this);
            try {
                persistentSubscription.get().getSubscriber().onComplete();
            } catch(Exception e) {
                log.error(format("Unexpected Exception while calling onComplete on Subscriber with type %s of Actor %s",
                    currentSubscription.getSubscriber() != null ?
                            currentSubscription.getSubscriber().getClass().getSimpleName() : null,
                    receiverRef), e);
            } finally {
                InternalSubscriberContext.getAndClearContext();
                persistentActor.removeSubscription(completedMessage.getMessageName(), publisherRef);
            }
        } else {
            // got a completed message but missing subscription
            log.error(format("Subscriber %s is missing PersistentSubscription for Publisher %s while handling CompletedMessage", receiverRef, publisherRef));
        }
    }

}
