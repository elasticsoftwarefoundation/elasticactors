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
 * This annotation should be used on a class that contains {@link MessageHandler} methods. The ElasticActors runtime will
 * scan all ElasticActors modules for this annotation and will then use the {@link MessageHandlersRegistry} to find
 * extension points. Can be used to implement a modular model where extra functionality for an actor is loaded from the
 * classpath at runtime.
 *
 * @author Joost van de Wijgerd
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PluggableMessageHandlers {
    /**
     * The {@link MethodActor} implementation that the {@link MessageHandler} methods defined by the class annotation with
     * the {@link PluggableMessageHandlers} annotation should be applied to. This can be used to extend the message
     * handling of an actor at runtime.
     *
     * @return the {@link MethodActor} implementation that the {@link MessageHandler} should be applied to.
     */
    Class<? extends MethodActor> value();
}
