/*
 * Copyright 2013 - 2014 The Original Authors
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

package org.elasticsoftware.elasticactors;

import org.elasticsoftware.elasticactors.serialization.Message;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * @author Joost van de Wijgerd
 */
public abstract class MethodActor extends TypedActor<Object> {
    private final Map<Class<?>,HandlerMethodDefinition> handlerCache = new HashMap<>();
    @Nullable private final Class<? extends ActorState> stateClass;

    protected MethodActor() {
        this.stateClass = resolveActorStateClass();
        // initialize the handler cache
        Method[] methods = getClass().getMethods();
        for (Method method : methods) {
            if(method.isAnnotationPresent(MessageHandler.class)) {
                HandlerMethodDefinition definition = new HandlerMethodDefinition(method);
                handlerCache.put(definition.messageClass,definition);
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
    public final void onReceive(ActorRef sender, Object message) throws Exception {
        HandlerMethodDefinition definition = handlerCache.get(message.getClass());
        if(definition != null) {
            try {
                definition.handlerMethod.invoke(this,definition.prepareParameters(sender,message));
            } catch (InvocationTargetException e) {
                final Throwable cause = e.getCause();
                if(Exception.class.isAssignableFrom(cause.getClass())) {
                    throw (Exception) cause;
                } else {
                    // this is some system error, don't swallow it but just rethrow the Invocation Target Exception
                    throw e;
                }
            }
        } else {
            onUnhandled(sender,message);
        }
    }

    protected void onUnhandled(ActorRef sender,Object message) {

    }

    private static enum ParameterType {
        MESSAGE,
        SENDER_REF,
        STATE,
        ACTOR_SYSTEM,
    }

    private final class HandlerMethodDefinition {
        private final Method handlerMethod;
        private final ParameterType[] parameterTypeOrdering;
        private final Class<?> messageClass;


        private HandlerMethodDefinition(Method handlerMethod) throws IllegalArgumentException, IllegalStateException {
            this.handlerMethod = handlerMethod;
            Class<?>[] parameterTypes = handlerMethod.getParameterTypes();
            // do some sanity checking
            if(parameterTypes.length == 0) {
                throw new IllegalArgumentException(format("Handler Method %s should have at least one parameter (message)",handlerMethod.toString()));
            }
            Class<?> messageParamaterClass = null;
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
                    messageParamaterClass = parameterTypes[i];
                } else {
                    throw new IllegalStateException(format("Unexpected Parameter Type %s",parameterTypes[i].getName()));
                }
            }
            if(messageParamaterClass == null) {
                throw new IllegalArgumentException(format("Handler Method %s should have at least one parameter annotated with @Message",handlerMethod.toString()));
            }
            this.messageClass = messageParamaterClass;
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
}
