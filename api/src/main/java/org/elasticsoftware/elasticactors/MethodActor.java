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

package org.elasticsoftware.elasticactors;

import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;
import org.elasticsoftware.elasticactors.state.PersistenceAdvisor;
import org.elasticsoftware.elasticactors.state.PersistenceConfig;
import org.elasticsoftware.elasticactors.state.PersistenceConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public abstract class MethodActor extends TypedActor<Object> implements PersistenceAdvisor {
    private static final Logger logger = LoggerFactory.getLogger(MethodActor.class);
    private static final MessageHandlerOrderComparator ORDER_COMPARATOR = new MessageHandlerOrderComparator();
    private final Map<Class<?>,List<HandlerMethodDefinition>> handlerCache = new HashMap<>();
    @Nullable private final Class<? extends ActorState> stateClass;

    protected MethodActor() {
        this.stateClass = resolveActorStateClass();
        // initialize the handler cache
        updateHandlerCache(getClass(),this);
        // also see if there are any other classes that have MessageHandler definitions
        MessageHandlers otherHandlers = getClass().getAnnotation(MessageHandlers.class);
        if(otherHandlers != null) {
            for (Class<?> aClass : otherHandlers.value()) {
                // avoid adding self again
                if(getClass() != aClass) {
                    updateHandlerCache(aClass,null);
                }
            }
            // see if we have a MessageHandlersRegistry
            Class<? extends MessageHandlersRegistry> registryClass = otherHandlers.registryClass();
            if(!MessageHandlers.NoopMessageHandlersRegistry.class.equals(registryClass)) {
                // try to instantiate the registry
                try {
                    MessageHandlersRegistry registry = registryClass.newInstance();
                    registry.init();
                    List<Class<?>> messageHandlers = registry.getMessageHandlers(getClass());
                    if(messageHandlers != null) {
                        for (Class<?> messageHandler : messageHandlers) {
                            updateHandlerCache(messageHandler,null);
                        }
                    }
                } catch(Exception e) {
                    logger.error("Exception while instantiating MessageHandlersRegistry of type [{}]",registryClass.getName(),e);
                }
            }
        }
        // order the MessageHandlers
        orderHandlerCache();
    }

    @Override
    public final boolean shouldUpdateState(Object message) {
        // need to take into account the loaded handlers here
        final List<HandlerMethodDefinition> definitions = handlerCache.get(message.getClass());
        if(definitions != null) {
            boolean configFound = false;
            for (HandlerMethodDefinition definition : definitions) {
                // see if we have a @PersistenceConfig on the declaring class
                PersistenceConfig persistenceConfig = definition.handlerMethod.getDeclaringClass().getAnnotation(PersistenceConfig.class);
                // if we need to persist, return
                if(persistenceConfig != null) {
                    configFound = true;
                    if(PersistenceConfigHelper.shouldUpdateState(persistenceConfig,message)) {
                        return true;
                    }
                }
                // else search further
            }
            // if we are here, and we have a config found, then return false
            if(configFound) {
                return false;
            }
        }
        // if we are here, we might be dealing with a MessageHandlers class that is not annotated, so look the
        // MethodActor itself
        return PersistenceConfigHelper.shouldUpdateState(getClass().getAnnotation(PersistenceConfig.class),message);
    }

    @Override
    public final boolean shouldUpdateState(ActorLifecycleStep lifecycleStep) {
        // this one only looks at the MethodActor implementation, not at the loaded handlers as they shouldn't define
        // lifecycle behavior
        final PersistenceConfig persistenceConfig = getClass().getAnnotation(PersistenceConfig.class);
        return PersistenceConfigHelper.shouldUpdateState(persistenceConfig,lifecycleStep);
    }

    private void orderHandlerCache() {
        for (List<HandlerMethodDefinition> definitions : handlerCache.values()) {
            definitions.sort(ORDER_COMPARATOR);
        }
    }

    private void updateHandlerCache(Class<?> clazz,@Nullable Object instance) {
        final Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            MessageHandler messageHandlerAnnotation = method.getAnnotation(MessageHandler.class);
            if(messageHandlerAnnotation != null) {
                HandlerMethodDefinition definition;
                if(Modifier.isStatic(method.getModifiers())) {
                    definition = new HandlerMethodDefinition(null, method, messageHandlerAnnotation.order());
                } else {
                    if(instance == null) {
                        // try to create instance with no-args constructor
                        try {
                            instance = clazz.newInstance();
                        } catch(Exception e) {
                            throw new IllegalArgumentException(format("Cannot create instance of type %s",clazz.getName()),e);
                        }
                    }
                    definition = new HandlerMethodDefinition(instance, method, messageHandlerAnnotation.order());

                }
                List<HandlerMethodDefinition> definitions = handlerCache.computeIfAbsent(definition.messageClass, k -> new LinkedList<>());
                definitions.add(definition);
            }
        }
    }

    private Class<? extends ActorState> resolveActorStateClass() {
        Actor actorAnnotation = getClass().getAnnotation(Actor.class);
        if(actorAnnotation != null) {
            return actorAnnotation.stateClass();
        } else {
            TempActor tempActorAnnotation = this.getClass().getAnnotation(TempActor.class);
            if(tempActorAnnotation != null) {
                return tempActorAnnotation.stateClass();
            }
        }
        return null;
    }

    @Override
    public void onReceive(ActorRef sender, Object message) throws Exception {
        List<HandlerMethodDefinition> definitions = handlerCache.get(message.getClass());
        if(definitions != null) {
            for (HandlerMethodDefinition definition : definitions) {
                try {
                    definition.handlerMethod.invoke(definition.targetInstance,definition.prepareParameters(sender,message));
                } catch (InvocationTargetException e) {
                    final Throwable cause = e.getCause();
                    if(Exception.class.isAssignableFrom(cause.getClass())) {
                        // throw (Exception) cause;
                        logger.error("Unexpected Exception in handlerMethod '{}' for actor [{}]", definition.handlerMethod, getSelf(), cause);
                    } else {
                        // this is some system error, don't swallow it but just rethrow the Invocation Target Exception
                        // throw e;
                        logger.error("Unexpected InvocationTargetException in handlerMethod '{}' for actor [{}]", definition.handlerMethod, getSelf(), e);
                    }
                }
            }
        } else {
            onUnhandled(sender,message);
        }
    }

    protected void onUnhandled(ActorRef sender,Object message) {

    }

    private enum ParameterType {
        MESSAGE,
        SENDER_REF,
        STATE,
        ACTOR_SYSTEM,
    }

    private final class HandlerMethodDefinition {
        private final @Nullable Object targetInstance;
        private final Method handlerMethod;
        private final ParameterType[] parameterTypeOrdering;
        private final Class<?> messageClass;
        private final int order;

        private HandlerMethodDefinition(@Nullable Object targetInstance, Method handlerMethod, int order) throws IllegalArgumentException, IllegalStateException {
            this.targetInstance = targetInstance;
            this.handlerMethod = handlerMethod;
            this.order = order;
            Class<?>[] parameterTypes = handlerMethod.getParameterTypes();
            // do some sanity checking
            if(parameterTypes.length == 0) {
                throw new IllegalArgumentException(format("Handler Method %s should have at least one parameter (message)",handlerMethod.toString()));
            }
            Class<?> messageParameterClass = null;
            this.parameterTypeOrdering = new ParameterType[parameterTypes.length];
            for (int i = 0; i < parameterTypes.length; i++) {
                if(parameterTypes[i].equals(ActorRef.class)) {
                    parameterTypeOrdering[i] = ParameterType.SENDER_REF;
                } else if(parameterTypes[i].equals(ActorSystem.class)) {
                    parameterTypeOrdering[i] = ParameterType.ACTOR_SYSTEM;
                } else if(ActorState.class.isAssignableFrom(parameterTypes[i])) {
                    parameterTypeOrdering[i] = ParameterType.STATE;
                } else if(parameterTypes[i].isAnnotationPresent(Message.class)) {
                    parameterTypeOrdering[i] = ParameterType.MESSAGE;
                    messageParameterClass = parameterTypes[i];
                } else {
                    throw new IllegalStateException(format("Unexpected Parameter Type %s",parameterTypes[i].getName()));
                }
            }
            if(messageParameterClass == null) {
                throw new IllegalArgumentException(format("Handler Method %s should have at least one parameter annotated with @Message",handlerMethod.toString()));
            }
            this.messageClass = messageParameterClass;
        }

        private Object[] prepareParameters(ActorRef sender,Object message) {
            Object[] arguments = new Object[parameterTypeOrdering.length];
            for (int i = 0; i < parameterTypeOrdering.length; i++) {
                ParameterType parameterType = parameterTypeOrdering[i];
                switch(parameterType) {
                    case MESSAGE:
                        arguments[i] = message;
                        continue;
                    case SENDER_REF:
                        arguments[i] = sender;
                        continue;
                    case STATE:
                        arguments[i] = getState(MethodActor.this.stateClass);
                        continue;
                    case ACTOR_SYSTEM:
                        arguments[i] = getSystem();
                        continue;
                    default:
                        throw new IllegalStateException(format("ParameterType %s not handled!",parameterType.toString()));

                }

            }
            return arguments;
        }
    }

    private static final class MessageHandlerOrderComparator implements Comparator<HandlerMethodDefinition> {
        @Override
        public int compare(HandlerMethodDefinition o1, HandlerMethodDefinition o2) {
            return Integer.compare(o1.order, o2.order);
        }
    }
}
