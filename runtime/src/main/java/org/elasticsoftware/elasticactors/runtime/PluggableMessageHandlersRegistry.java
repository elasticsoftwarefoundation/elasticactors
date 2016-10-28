/*
 * Copyright 2013 - 2016 The Original Authors
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

package org.elasticsoftware.elasticactors.runtime;

import org.elasticsoftware.elasticactors.MessageHandlersRegistry;
import org.elasticsoftware.elasticactors.MethodActor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import java.util.List;

/**
 * Implementation of the {@link MessageHandlersRegistry} that autowires the {@link PluggableMessageHandlersScanner}
 * class to delegate the {@link MessageHandlersRegistry#getMessageHandlers(Class)} method to.
 *
 * @author Joost van de Wijgerd
 */
@Configurable
public final class PluggableMessageHandlersRegistry implements MessageHandlersRegistry {
    private MessageHandlersRegistry delegate;

    public PluggableMessageHandlersRegistry() {
    }

    @Autowired
    public void setDelegate(MessageHandlersRegistry delegate) {
        this.delegate = delegate;
    }

    @Override
    public void init() {
        // no need to do anything, should be autowired on construction
    }

    @Override
    public List<Class<?>> getMessageHandlers(Class<? extends MethodActor> methodActor) {
        return delegate.getMessageHandlers(methodActor);
    }
}
