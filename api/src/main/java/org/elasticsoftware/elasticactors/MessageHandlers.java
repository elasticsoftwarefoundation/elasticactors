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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation can be used on an Actor class that extends {@link MethodActor}. The specified classed in {@link #value()} will
 * be inspected and any {@link java.lang.reflect.Method}s on the class that have the {@link MessageHandler} annotation and conform
 * to the spec (i.e. have at least one parameter annotated with {@link org.elasticsoftware.elasticactors.serialization.Message}) will
 * be added to the handler cache and received messages will be deletad to the appropriate {@link MessageHandler}
 *
 * @author Joost van de Wijgerd
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface MessageHandlers {
    /**
     * Array of classes that will be inspected for {@link java.lang.reflect.Method}s that are annotated with
     * {@link MessageHandler}
     *
     * @return the array of classes that will be inspected
     */
    Class<?>[] value();
    /**
     * Another extension point that can be used to load classes that have {@link MessageHandler} methods at runtime.
     * The {@link MethodActor} will instantiate the class and then call
     * {@link org.elasticsoftware.elasticactors.MessageHandlersRegistry#init()},
     * subsequently the {@link org.elasticsoftware.elasticactors.MessageHandlersRegistry#getMessageHandlers(Class)} will be
     * used to obtain a list of classes.
     *
     * @return another extension point that can be used to load classes that have {@link MessageHandler} methods at runtime
     */
    Class<? extends  MessageHandlersRegistry> registryClass() default NoopMessageHandlersRegistry.class;

    /**
     * Default (null) MessageHandlersRegistry
     */
    abstract class NoopMessageHandlersRegistry implements MessageHandlersRegistry {}
}
