/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Implementations are used in the construction of a {@link MethodActor} to find additional {@link MessageHandler} methods.
 *
 * @author Joost van de Wijgerd
 */
public interface MessageHandlersRegistry {
    /**
     * Initialize the registry, the framework will call {MessageHandlersRegistry#getMessageHandlers} next
     */
    public void init();
    /**
     * Return classes that are annotated with {@link PluggableMessageHandlers} which have the
     * {@link org.elasticsoftware.elasticactors.PluggableMessageHandlers#value()} set to the given parameter type
     *
     * @param methodActor actor type for which to get {@link PluggableMessageHandlers} for
     * @return classes that are annotated with {@link PluggableMessageHandlers} for the actor type
     */
    @Nonnull
    List<Class<?>> getMessageHandlers(Class<? extends MethodActor> methodActor);
}
