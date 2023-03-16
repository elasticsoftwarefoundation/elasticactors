/*
 * Copyright 2013 - 2023 The Original Authors
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

package org.elasticsoftware.elasticactors.kafka.cluster;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.PublisherNotFoundException;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.CancelMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.CompletedMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.NextMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.RequestMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.SubscribeMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.SubscriptionMessage;
import org.elasticsoftware.elasticactors.reactivestreams.InternalPersistentSubscription;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;
import org.elasticsoftware.elasticactors.state.MessageSubscriber;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;

import static org.elasticsoftware.elasticactors.cluster.tasks.ActorLifecycleTask.shouldUpdateState;
import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;
import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

public final class ReactiveStreamsProtocol {
    private static final Logger logger = LoggerFactory.getLogger(ReactiveStreamsProtocol.class);

    public static Boolean handleUndeliverableMessage(InternalActorSystem actorSystem,
                                                     PersistentActor persistentActor,
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
                    InternalPersistentSubscription currentSubscription = persistentSubscription.get();
                    InternalSubscriberContext.setContext(
                            new SubscriberContextImpl(persistentActor, internalMessage.getSender(), actorSystem, currentSubscription));
                    try {
                        currentSubscription.getSubscriber().onError(
                                new PublisherNotFoundException(String.format("Actor[%s] does not exist",
                                        internalMessage.getSender().toString()), internalMessage.getSender()));
                    } catch(Exception e) {
                        logger.error("Unexpected Exception while calling onError on Subscriber with type {} of Actor {}",
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
            logger.error("Exception while Deserializing Message class {} in ActorSystem [{}]",
                    internalMessage.getPayloadClass(), actorSystem.getName(), e);

        }
        return false;
    }

    public static Boolean handleMessage(InternalActorSystem actorSystem,
                                        PersistentActor persistentActor,
                                        ElasticActor receiver,
                                        ActorRef receiverRef,
                                        InternalMessage internalMessage) {
        try {
            Object message = deserializeMessage(actorSystem, internalMessage);
            if(message instanceof NextMessage) {
                return handle((NextMessage) message, persistentActor, receiver, receiverRef, internalMessage.getSender(), actorSystem);
            } else if(message instanceof SubscribeMessage) {
                handle((SubscribeMessage) message, receiverRef, ((SubscribeMessage) message).getSubscriberRef(), persistentActor);
            } else if(message instanceof CancelMessage) {
                handle((CancelMessage) message, ((CancelMessage) message).getSubscriberRef(), persistentActor);
            } else if(message instanceof RequestMessage) {
                handle((RequestMessage) message, internalMessage.getSender(), persistentActor);
            } else if(message instanceof SubscriptionMessage) {
                handle((SubscriptionMessage) message, receiverRef, internalMessage.getSender(), persistentActor, actorSystem);
            } else if(message instanceof CompletedMessage) {
                handle((CompletedMessage) message, receiverRef, internalMessage.getSender(), persistentActor, actorSystem);
            }
            return true;
        } catch (Exception e) {
            logger.error("Exception while Deserializing Message class {} in ActorSystem [{}]",
                    internalMessage.getPayloadClass(), actorSystem.getName(), e);
            return false;
        }
    }

    private static Boolean handle(NextMessage nextMessage,
                                  PersistentActor persistentActor,
                                  ElasticActor receiver,
                                  ActorRef receiverRef,
                                  ActorRef publisherRef,
                                  InternalActorSystem actorSystem) {
        Optional<InternalPersistentSubscription> persistentSubscription =
                persistentActor.getSubscription(nextMessage.getMessageName(), publisherRef);

        if(persistentSubscription.isPresent()) {
            InternalPersistentSubscription currentSubscription = persistentSubscription.get();
            InternalSubscriberContext.setContext(new SubscriberContextImpl(persistentActor, publisherRef, actorSystem, currentSubscription));
            try {
                // @todo: for now the message name == messageClass
                Class<?> messageClass = getClassHelper().forName(nextMessage.getMessageName());
                MessageDeserializer<?> deserializer = actorSystem.getDeserializer(messageClass);
                Object message = SerializationContext.deserialize(deserializer, ByteBuffer.wrap(nextMessage.getMessageBytes()));

                currentSubscription.getSubscriber().onNext(message);

                return shouldUpdateState(receiver, message);
            } catch (ClassNotFoundException e) {
                // the message type (class) that I am subscribing to is not available
                logger.error("Actor[{}]: Could not find message type: <{}>, unable to deserialize subscribed message", receiverRef, nextMessage.getMessageName());
            } catch (IOException e) {
                logger.error("Actor[{}]: Problem trying to deserialize message embedded in NextMessage", receiverRef, e);
            } catch (Exception e) {
                logger.error("Unexpected Exception while calling onNext on Subscriber with type {} of Actor {}",
                        currentSubscription.getSubscriber() != null ?
                                currentSubscription.getSubscriber().getClass().getSimpleName() : null,
                        receiverRef, e);
            } finally {
                InternalSubscriberContext.clearContext();
            }
        } else {
            // there is no corresponding persistent subscription related to this publisher/message combination
            // this should not happen, however if it does happen we need to cancel the subscriber on the publisher side
            logger.error("Subscriber {} is missing PersistentSubscription for Publisher {} while handling NextMessage", receiverRef, publisherRef);
            publisherRef.tell(new CancelMessage(receiverRef, nextMessage.getMessageName()));

        }
        return false;
    }

    private static void handle(SubscribeMessage subscribeMessage, ActorRef receiverRef, ActorRef subscriberRef, PersistentActor persistentActor) {
        persistentActor.addSubscriber(subscribeMessage.getMessageName(), new MessageSubscriber(subscriberRef));
        // let the subscriber know he has a subscription
        subscriberRef.tell(new SubscriptionMessage(subscribeMessage.getMessageName()), receiverRef);
    }

    private static void handle(CancelMessage cancelMessage, ActorRef subscriberRef, PersistentActor persistentActor) {
        if(persistentActor.removeSubscriber(cancelMessage.getMessageName(), new MessageSubscriber(subscriberRef))) {
            // let the subscriber know his subscription was cancelled (completed)
            subscriberRef.tell(new CompletedMessage(cancelMessage.getMessageName()));
        }
    }

    private static void handle(RequestMessage requestMessage, ActorRef subscriberRef, PersistentActor persistentActor) {
        if(persistentActor.getMessageSubscribers() != null) {
            ((Set<MessageSubscriber>)persistentActor.getMessageSubscribers().get(requestMessage.getMessageName()))
                    .stream().filter(m -> m.getSubscriberRef().equals(subscriberRef)).findFirst()
                    .ifPresent(messageSubscriber -> messageSubscriber.incrementAndGet(requestMessage.getN()));
        }
    }

    private static void handle(SubscriptionMessage subscriptionMessage,
                               ActorRef subscriberRef,
                               ActorRef publisherRef,
                               PersistentActor persistentActor,
                               InternalActorSystem actorSystem) {
        Optional<InternalPersistentSubscription> persistentSubscription =
                persistentActor.getSubscription(subscriptionMessage.getMessageName(), publisherRef);
        if(persistentSubscription.isPresent()) {
            InternalPersistentSubscription currentSubscription = persistentSubscription.get();
            // notify the subscriber
            InternalSubscriberContext.setContext(new SubscriberContextImpl(persistentActor, publisherRef, actorSystem, currentSubscription));
            try {
                persistentSubscription.get().getSubscriber().onSubscribe(persistentSubscription.get());
            } catch(Exception e) {
                logger.error("Unexpected Exception while calling onSubscribe on Subscriber with type {} of Actor {}",
                        currentSubscription.getSubscriber() != null ?
                                currentSubscription.getSubscriber().getClass().getSimpleName() : null,
                        subscriberRef, e);
            } finally {
                InternalSubscriberContext.clearContext();
            }
        } else {
            // we got a subscription message, but there is no corresponding PersistentSubscription ... this should not
            // be possible. however since the other side now has a corresponding subscriber reference we need to cancel
            // it to keep
            logger.error("Subscriber {} is missing PersistentSubscription for Publisher {} while handling SubscriptionMessage", subscriberRef, publisherRef);
            publisherRef.tell(new CancelMessage(subscriberRef, subscriptionMessage.getMessageName()));
        }

    }

    private static void handle(CompletedMessage completedMessage,
                               ActorRef subscriberRef,
                               ActorRef publisherRef,
                               PersistentActor persistentActor,
                               InternalActorSystem actorSystem) {
        Optional<InternalPersistentSubscription> persistentSubscription =
                persistentActor.getSubscription(completedMessage.getMessageName(), publisherRef);
        if(persistentSubscription.isPresent()) {
            InternalPersistentSubscription currentSubscription = persistentSubscription.get();
            InternalSubscriberContext.setContext(new SubscriberContextImpl(persistentActor, publisherRef, actorSystem, currentSubscription));
            try {
                persistentSubscription.get().getSubscriber().onComplete();
            } catch(Exception e) {
                logger.error("Unexpected Exception while calling onComplete on Subscriber with type {} of Actor {}",
                        currentSubscription.getSubscriber() != null ?
                                currentSubscription.getSubscriber().getClass().getSimpleName() : null,
                        subscriberRef, e);
            } finally {
                InternalSubscriberContext.clearContext();
                persistentActor.removeSubscription(completedMessage.getMessageName(), publisherRef);
            }
        } else {
            // got a completed message but missing subscription
            logger.error("Subscriber {} is missing PersistentSubscription for Publisher {} while handling CompletedMessage", subscriberRef, publisherRef);
        }
    }

}

