package org.elasticsoftware.elasticactors.kafka.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.*;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.messaging.reactivestreams.NextMessage;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;
import org.elasticsoftware.elasticactors.state.MessageSubscriber;
import org.elasticsoftware.elasticactors.state.PersistentActor;
import org.elasticsoftware.elasticactors.util.SerializationTools;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import static java.lang.String.format;
import static org.elasticsoftware.elasticactors.cluster.tasks.ActorLifecycleTask.shouldUpdateState;
import static org.elasticsoftware.elasticactors.util.SerializationTools.deserializeMessage;

public final class ApplicationProtocol {
    private static final Logger logger = LogManager.getLogger(ApplicationProtocol.class);

    private static void executeLifecycleListeners(InternalActorSystem actorSystem,
                                                  PersistentActor persistentActor,
                                                  BiConsumer<ActorLifecycleListener, PersistentActor> lifecycleConsumer) {
        final List<ActorLifecycleListener<?>> lifecycleListeners = actorSystem.getActorLifecycleListeners(persistentActor.getActorClass());
        if(lifecycleListeners != null) {
            for (ActorLifecycleListener<?> lifecycleListener : lifecycleListeners) {
                try {
                    lifecycleConsumer.accept(lifecycleListener, persistentActor);
                } catch(Throwable t) {
                    logger.warn("Exception while executing ActorLifecycleListener",t);
                }
            }
        }
    }

    public static Boolean activateActor(InternalActorSystem actorSystem,
                                        PersistentActor persistentActor,
                                        ElasticActor receiver,
                                        ActorRef receiverRef,
                                        InternalMessage internalMessage) {
        boolean overridePersistenceConfig = false;
        // need to deserialize the state here (unless there is none)
        if (persistentActor.getSerializedState() != null) {
            final SerializationFramework framework = SerializationTools.getSerializationFramework(actorSystem.getParent(), receiver.getClass());
            try {
                ActorState actorState = receiver.preActivate(persistentActor.getPreviousActorStateVersion(),
                        persistentActor.getCurrentActorStateVersion(),
                        persistentActor.getSerializedState(),
                        framework);
                if (actorState == null) {
                    actorState = SerializationTools.deserializeActorState(actorSystem.getParent(),
                                                                          receiver.getClass(),
                                                                          persistentActor.getSerializedState());
                } else {
                    overridePersistenceConfig = true;
                }
                // set state and remove bytes
                persistentActor.setState(actorState);
                persistentActor.setSerializedState(null);
            } catch (Exception e) {
                logger.error(format("Exception calling preActivate for actorId [%s]", receiverRef.getActorId()), e);
            }
        }

        try {
            receiver.postActivate(persistentActor.getPreviousActorStateVersion());
            executeLifecycleListeners(actorSystem, persistentActor, (a, p) -> a.postActivate(p.getSelf(), p.getState(), p.getPreviousActorStateVersion()));
        } catch (Exception e) {
            logger.error(format("Exception calling postActivate for actorId [%s]", receiverRef.getActorId()), e);
            return false;
        }
        // when the preActivate has a result we should always store the state
        // check persistence config (if any)
        return overridePersistenceConfig || shouldUpdateState(receiver, ActorLifecycleStep.ACTIVATE);
    }

    public static Boolean passivateActor(InternalActorSystem actorSystem,
                                         PersistentActor persistentActor,
                                         ElasticActor receiver,
                                         ActorRef receiverRef,
                                         InternalMessage internalMessage) {
        try {
            receiver.prePassivate();
            executeLifecycleListeners(actorSystem, persistentActor, (a, p) -> a.prePassivate(p.getSelf(), p.getState()));
        } catch (Exception e) {
            logger.error("Exception calling prePassivate",e);
        }
        // check persistence config (if any) -> by default return false
        return shouldUpdateState(receiver,ActorLifecycleStep.PASSIVATE);
    }

    public static Boolean createActor(InternalActorSystem actorSystem,
                                      PersistentActor persistentActor,
                                      ElasticActor receiver,
                                      ActorRef receiverRef,
                                      @Nullable InternalMessage internalMessage) {
        if(logger.isDebugEnabled()) {
            logger.debug(String.format("Creating Actor for ref [%s] of type [%s]",receiverRef.toString(), receiver.getClass().getName()));
        }
        try {
            // the creator is the sender of the internalMessage
            receiver.postCreate(internalMessage == null ? null : internalMessage.getSender());
            executeLifecycleListeners(actorSystem, persistentActor, (a, p) -> a.postCreate(p.getSelf(), p.getState()));
            // no previousversion as this is new
            receiver.postActivate(null);
            executeLifecycleListeners(actorSystem, persistentActor, (a, p) -> a.postActivate(p.getSelf(), p.getState(),null));

        } catch (Exception e) {
            logger.error("Exception calling postCreate",e);
        }
        return shouldUpdateState(receiver, ActorLifecycleStep.CREATE);
    }

    public static Boolean destroyActor(InternalActorSystem actorSystem,
                                       PersistentActor persistentActor,
                                       ElasticActor receiver,
                                       ActorRef receiverRef,
                                       InternalMessage internalMessage) {
        if(logger.isDebugEnabled()) {
            logger.debug(String.format("Destroying Actor for ref [%s] of type [%s]",receiverRef.toString(),receiver.getClass().getName()));
        }
        try {
            // @todo: figure out the destroyer
            receiver.preDestroy(null);
            notifyPublishers(persistentActor);
            notifySubscribers(persistentActor, internalMessage, receiverRef, actorSystem);
            executeLifecycleListeners(actorSystem, persistentActor, (a, p) -> a.preDestroy(p.getSelf(), p.getState()));
        } catch (Exception e) {
            logger.error("Exception calling preDestroy",e);
        }
        // never update record (entry has been deleted)
        return false;
    }

    public static Boolean handleUndeliverableMessage(InternalActorSystem actorSystem,
                                                     PersistentActor persistentActor,
                                                     ElasticActor receiver,
                                                     ActorRef receiverRef,
                                                     InternalMessage internalMessage) {
        try {
            Object message = deserializeMessage(actorSystem, internalMessage);
            try {
                receiver.onUndeliverable(internalMessage.getSender(), message);
                return shouldUpdateState(receiver,message);
            } catch(MessageDeliveryException e) {
                // see if it is a recoverable exception
                if(!e.isRecoverable()) {
                    logger.error(format("Unrecoverable MessageDeliveryException while handling message for actor [%s]", receiverRef.toString()), e);
                }
                // message cannot be sent but state should be updated as the received message did most likely change
                // the state
                return shouldUpdateState(receiver, message);
            } catch (Exception e) {
                logger.error(String.format("Exception while handling undeliverable message for actor [%s]", receiverRef.toString()), e);
                return false;
            }
        } catch (Exception e) {
            logger.error(String.format("Exception while Deserializing Message class %s in ActorSystem [%s]",
                    internalMessage.getPayloadClass(), actorSystem.getName()), e);
            return false;
        }
    }

    public static Boolean handleMessage(InternalActorSystem actorSystem,
                                        PersistentActor persistentActor,
                                        ElasticActor receiver,
                                        ActorRef receiverRef,
                                        InternalMessage internalMessage) {
        try {
            Object message = deserializeMessage(actorSystem, internalMessage);
            try {
                receiver.onReceive(internalMessage.getSender(), message);
                // reactive streams
                notifySubscribers(persistentActor, internalMessage, receiverRef, actorSystem);
                return shouldUpdateState(receiver, message);
            } catch(MessageDeliveryException e) {
                // see if it is a recoverable exception
                if(!e.isRecoverable()) {
                    logger.error(format("Unrecoverable MessageDeliveryException while handling message for actor [%s]", receiverRef.toString()), e);
                }
                // message cannot be sent but state should be updated as the received message did most likely change
                // the state
                return shouldUpdateState(receiver, message);

            } catch (Exception e) {
                logger.error(format("Exception while handling message for actor [%s]", receiverRef.toString()), e);
                return false;
            }
        } catch (Exception e) {
            logger.error(format("Exception while Deserializing Message class %s in ActorSystem [%s]",
                    internalMessage.getPayloadClass(), actorSystem.getName()), e);
            return false;
        }
    }

    private static void notifySubscribers(PersistentActor persistentActor,
                                          InternalMessage internalMessage,
                                          ActorRef receiverRef,
                                          InternalActorSystem actorSystem) {
        if(persistentActor.getMessageSubscribers() != null) {
            try {
                // todo consider using ActorRefGroup here
                if(persistentActor.getMessageSubscribers().containsKey(internalMessage.getPayloadClass())) {
                    // copy the bytes from the incoming message, discarding possible changes made in onReceive
                    NextMessage nextMessage = new NextMessage(internalMessage.getPayloadClass(), getMessageBytes(internalMessage, actorSystem));
                    ((Set<MessageSubscriber>) persistentActor.getMessageSubscribers().get(internalMessage.getPayloadClass()))
                            .stream().filter(messageSubscriber -> messageSubscriber.getAndDecrement() > 0)
                            .forEach(messageSubscriber -> messageSubscriber.getSubscriberRef().tell(nextMessage, receiverRef));
                }
            } catch(Exception e) {
                logger.error("Unexpected exception while forwarding message to Subscribers", e);
            }
        }
    }

    private static void notifyPublishers(PersistentActor persistentActor) {
        // we need to tell our publishers to stop publising.. they will send a completed message
        // that wull fail but this should be no problem
        try {
            persistentActor.cancelAllSubscriptions();
        } catch(Exception e) {
            logger.error("Unexpected Exception while cancelling subscriptions", e);
        }
    }

    private static byte[] getMessageBytes(InternalMessage internalMessage, InternalActorSystem actorSystem) throws IOException {
        if(internalMessage.getPayload() != null) {
            if(internalMessage.getPayload().hasArray()) {
                return internalMessage.getPayload().array();
            } else {
                // make sure we are at the start of the payload bytes
                internalMessage.getPayload().rewind();
                byte[] messageBytes = new byte[internalMessage.getPayload().remaining()];
                internalMessage.getPayload().get(messageBytes);
                return messageBytes;
            }
        } else {
            // transient message, need to serialize the bytes
            Object message = internalMessage.getPayload(null);
            ByteBuffer messageBytes = ((MessageSerializer<Object>) actorSystem.getSerializer(message.getClass())).serialize(message);
            if(messageBytes.hasArray()) {
                return messageBytes.array();
            } else {
                byte[] bytes = new byte[internalMessage.getPayload().remaining()];
                messageBytes.get(bytes);
                return bytes;
            }
        }
    }
}
