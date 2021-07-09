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
 * Marks a persistent actor as a Singleton Actor (i.e. one, and only one instance will exist).
 * <br>
 * The framework will guarantee that:
 * <ol>
 * <li>If not created yet, an actor of this type and with this ID will be created</li>
 * <li>This actor will always be activated when its shard is initialized</li>
 * <li>No other actors of this type will be allowed to be created</li>
 * </ol>
 * The initial state of this actor will be determined by the
 * {@link SingletonActor#initialStateProvider()} parameter.
 * <br>
 * The default implementation uses the default constructor of the state class.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SingletonActor {

    /**
     * The Actor ID
     *
     * @return the Actor ID
     */
    String value();

    /**
     * Implementation of {@link InitialStateProvider} used to create the initial actor state.
     * The default implementation uses the default no-args constructor in the state class.
     *
     * @return the implementation of {@link InitialStateProvider} used to create the initial actor state.
     */
    Class<? extends InitialStateProvider> initialStateProvider() default InitialStateProvider.Default.class;
}
