/*
 *   Copyright 2013 - 2022 The Original Authors
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

import java.util.List;

/**
 * Implementations are used to build instances of classes annotated with {@link SingletonActor} and {@link ManagedActor}
 */
public interface ManagedActorsRegistry {

    /**
     * Initialize the registry, the framework will call {@link ManagedActorsRegistry#getSingletonActorClasses} next
     */
    void init();

    /**
     * Return classes that are annotated with {@link SingletonActor}
     *
     * @return the classes that are annotated with {@link SingletonActor}
     */
    List<Class<? extends ElasticActor<?>>> getSingletonActorClasses();

    /**
     * Return classes that are annotated with {@link ManagedActor}
     *
     * @return the classes that are annotated with {@link ManagedActor}
     */
    List<Class<? extends ElasticActor<?>>> getManagedActorClasses();
}
