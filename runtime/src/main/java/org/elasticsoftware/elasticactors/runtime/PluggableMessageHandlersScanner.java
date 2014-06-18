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

package org.elasticsoftware.elasticactors.runtime;

import com.google.common.base.Supplier;
import com.google.common.collect.*;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.MessageHandlersRegistry;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.PluggableMessageHandlers;
import org.elasticsoftware.elasticactors.serialization.Message;
import org.elasticsoftware.elasticactors.serialization.SerializationFramework;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Find all classes annotated with {@link org.elasticsoftware.elasticactors.serialization.Message} and
 * register them with the {@link org.elasticsoftware.elasticactors.serialization.SerializationFramework#register(Class)}
 *
 * @author Joost van de Wijgerd
 */
@Named
public final class PluggableMessageHandlersScanner implements MessageHandlersRegistry {
    @Inject
    private ApplicationContext applicationContext;
    private final ListMultimap<Class<? extends MethodActor>,Class<?>> registry = LinkedListMultimap.create();


    @PostConstruct
    public void init() {
        String[] basePackages = ScannerHelper.findBasePackagesOnClasspath(applicationContext.getClassLoader());
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

        for (String basePackage : basePackages) {
            configurationBuilder.addUrls(ClasspathHelper.forPackage(basePackage));
        }

        Reflections reflections = new Reflections(configurationBuilder);

        Set<Class<?>> handlerClasses = reflections.getTypesAnnotatedWith(PluggableMessageHandlers.class);

        for (Class<?> handlerClass : handlerClasses) {
            PluggableMessageHandlers handlerAnnotation = handlerClass.getAnnotation(PluggableMessageHandlers.class);
            registry.put(handlerAnnotation.value(),handlerClass);
        }
    }

    @Override
    public List<Class<?>> getMessageHandlers(Class<? extends MethodActor> methodActor) {
        return registry.get(methodActor);
    }
}
