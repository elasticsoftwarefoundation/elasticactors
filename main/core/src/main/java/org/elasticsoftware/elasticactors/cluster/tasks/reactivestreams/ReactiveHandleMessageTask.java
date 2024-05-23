/*
 * Copyright 2013 - 2024 The Original Authors
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

package org.elasticsoftware.elasticactors.cluster.tasks.reactivestreams;

import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorState;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.PersistentSubscription;
import org.elasticsoftware.elasticactors.SubscriberContext;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.cluster.logging.LoggingSettings;
import org.elasticsoftware.elasticactors.cluster.metrics.MetricsSettings;
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
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.SerializationContext;
import org.elasticsoftware.elasticactors.state.ActorStateUpdateProcessor;
import org.elasticsoftware.elasticactors.state.MessageSubscriber;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.elasticsoftware.elasticactors.util.concurrent.MessageHandlingThreadBoundRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;

import static org.elasticsoftware.elasticactors.util.ClassLoadingHelper.getClassHelper;
import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

/**
 * Task that is responsible for internalMessage deserialization, error handling and state updates
 *
 * @author Joost van de Wijged
 */
final class ReactiveHandleMessageTask
    extends ActorLifecycleTask
    implements SubscriberContext, MessageHandlingThreadBoundRunnable<String> {

    private static final Logger log = LoggerFactory.getLogger(ReactiveHandleMessageTask.class);
    private InternalPersistentSubscription currentSubscription;
    private Object deserializedMessage;

    ReactiveHandleMessageTask(
        InternalActorSystem actorSystem,
        ElasticActor receiver,
        ActorRef receiverRef,
        InternalMessage internalMessage,
        PersistentActor persistentActor,
        PersistentActorRepository persistentActorRepository,
        ActorStateUpdateProcessor actorStateUpdateProcessor,
        MessageHandlerEventListener messageHandlerEventListener,
        MetricsSettings metricsSettings,
        LoggingSettings loggingSettings)
    {
        super(
            actorStateUpdateProcessor,
            persistentActorRepository,
            persistentActor,
            actorSystem,
            receiver,
            receiverRef,
            messageHandlerEventListener,
            internalMessage,
            metricsSettings,
            loggingSettings
        );
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
    @Nullable
    protected Class unwrapMessageClass(InternalMessage internalMessage)  {
        if (unwrappedMessageClass != null) {
            return unwrappedMessageClass;
        }
        if(NextMessage.class.getName().equals(internalMessage.getPayloadClass())) {
            try {
                NextMessage nextMessage = internalMessage.getPayload(actorSystem.getDeserializer(NextMessage.class));
                deserializedMessage = nextMessage;
                unwrappedMessageClass = getClassHelper().forName(nextMessage.getMessageName());
                return unwrappedMessageClass;
            } catch(IOException e) {
                log.error("Class [{}] could not be loaded", internalMessage.getPayloadClass(), e);
                return null;
            } catch(ClassNotFoundException e) {
                log.error("Class [{}] not found", internalMessage.getPayloadClass(), e);
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    protected boolean doInActorContext(InternalActorSystem actorSystem,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        try {
            Object message = deserializeAndCacheMessage(actorSystem, internalMessage);
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
            log.error("Exception while Deserializing Message class {} in ActorSystem [{}]",
                    internalMessage.getPayloadClass(), actorSystem.getName(), e);
            return false;
        }
    }

    private Object deserializeAndCacheMessage(
        InternalActorSystem actorSystem,
        InternalMessage internalMessage) throws Exception
    {
        if (deserializedMessage == null) {
            deserializedMessage = deserializeMessage(actorSystem, internalMessage);
        }
        return deserializedMessage;
    }

    private boolean handle(NextMessage nextMessage, ElasticActor receiver, ActorRef publisherRef, InternalActorSystem actorSystem) {
        Optional<InternalPersistentSubscription> persistentSubscription =
                persistentActor.getSubscription(nextMessage.getMessageName(), publisherRef);

        if(persistentSubscription.isPresent()) {
            try {
                currentSubscription = persistentSubscription.get();
                InternalSubscriberContext.setContext(this);
                // @todo: for now the message name == messageClass
                Class<?> messageClass = getClassHelper().forName(nextMessage.getMessageName());
                MessageDeserializer<?> deserializer = actorSystem.getDeserializer(messageClass);
                Object message = SerializationContext.deserialize(deserializer, ByteBuffer.wrap(nextMessage.getMessageBytes()));

                logMessageContents(message);
                currentSubscription.getSubscriber().onNext(message);

                return shouldUpdateState(receiver, message);
            } catch (ClassNotFoundException e) {
                // the message type (class) that I am subscribing to is not available
                log.error("Actor[{}]: Could not find message type: <{}>, unable to deserialize subscribed message", receiverRef, nextMessage.getMessageName());
            } catch (IOException e) {
                log.error("Actor[{}]: Problem trying to deserialize message embedded in NextMessage", receiverRef, e);
            } catch (Exception e) {
                log.error("Unexpected Exception while calling onNext on Subscriber with type {} of Actor {}",
                        currentSubscription.getSubscriber() != null ?
                                currentSubscription.getSubscriber().getClass().getSimpleName() : null,
                        receiverRef, e);
            } finally {
                InternalSubscriberContext.clearContext();
            }
        } else {
            // there is no corresponding persistent subscription related to this publisher/message combination
            // this should not happen, however if it does happen we need to cancel the subscriber on the publisher side
            log.error("Subscriber {} is missing PersistentSubscription for Publisher {} while handling NextMessage", receiverRef, publisherRef);
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
                log.error("Unexpected Exception while calling onSubscribe on Subscriber with type {} of Actor {}",
                        currentSubscription.getSubscriber() != null ?
                                currentSubscription.getSubscriber().getClass().getSimpleName() : null,
                        receiverRef, e);
            } finally {
                InternalSubscriberContext.clearContext();
            }
        } else {
            // we got a subscription message, but there is no corresponding PersistentSubscription ... this should not
            // be possible. however since the other side now has a corresponding subscriber reference we need to cancel
            // it to keep
            log.error("Subscriber {} is missing PersistentSubscription for Publisher {} while handling SubscriptionMessage", receiverRef, publisherRef);
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
                log.error("Unexpected Exception while calling onComplete on Subscriber with type {} of Actor {}",
                    currentSubscription.getSubscriber() != null ?
                            currentSubscription.getSubscriber().getClass().getSimpleName() : null,
                    receiverRef, e);
            } finally {
                InternalSubscriberContext.clearContext();
                persistentActor.removeSubscription(completedMessage.getMessageName(), publisherRef);
            }
        } else {
            // got a completed message but missing subscription
            log.error("Subscriber {} is missing PersistentSubscription for Publisher {} while handling CompletedMessage", receiverRef, publisherRef);
        }
    }

    @Override
    protected boolean shouldLogMessageInformation() {
        return true;
    }

    @Override
    public Class<? extends ElasticActor> getActorType() {
        return receiver.getClass();
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
