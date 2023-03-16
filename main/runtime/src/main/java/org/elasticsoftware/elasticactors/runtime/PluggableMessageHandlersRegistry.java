/*
 * Copyright 2013 - 2023 The Original Authors
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

package org.elasticsoftware.elasticactors.runtime;

import org.elasticsoftware.elasticactors.MessageHandlersRegistry;
import org.elasticsoftware.elasticactors.MethodActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Implementation of the {@link MessageHandlersRegistry} that autowires the {@link PluggableMessageHandlersScanner}
 * class to delegate the {@link MessageHandlersRegistry#getMessageHandlers(Class)} method to.
 *
 * @author Joost van de Wijgerd
 */
@Configurable
public final class PluggableMessageHandlersRegistry implements MessageHandlersRegistry {
    private final static Logger logger = LoggerFactory.getLogger(PluggableMessageHandlersRegistry.class);

    private MessageHandlersRegistry delegate;

    public PluggableMessageHandlersRegistry() {
        logger.debug("Instantiating [{}]", getClass().getName());
    }

    @Autowired
    public void setDelegate(MessageHandlersRegistry delegate) {
        logger.debug("Setting delegate [{}] for [{}]", delegate.getClass(), getClass().getName());
        this.delegate = delegate;
    }

    @Override
    public void init() {
        // no need to do anything, should be autowired on construction
    }

    @Override
    public List<Class<?>> getMessageHandlers(Class<? extends MethodActor> methodActor) {
        if (delegate != null) {
            return delegate.getMessageHandlers(methodActor);
        } else {
            logger.error("Instance of [{}] does not have a delegate", getClass().getName());
            return emptyList();
        }
    }
}
