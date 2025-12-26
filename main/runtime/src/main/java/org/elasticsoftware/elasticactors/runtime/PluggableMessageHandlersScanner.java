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

package org.elasticsoftware.elasticactors.runtime;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import jakarta.annotation.PostConstruct;
import org.elasticsoftware.elasticactors.MessageHandlersRegistry;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.PluggableMessageHandlers;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Find all classes annotated with {@link PluggableMessageHandlers} and add them to the registry
 *
 * @author Joost van de Wijgerd
 */
public final class PluggableMessageHandlersScanner implements MessageHandlersRegistry {
    private static final Logger logger = LoggerFactory.getLogger(PluggableMessageHandlersScanner.class);
    private final ApplicationContext applicationContext;
    private final ListMultimap<Class<? extends MethodActor>,Class<?>> registry = LinkedListMultimap.create();

    public PluggableMessageHandlersScanner(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    @PostConstruct
    public synchronized void init() {
        logger.info("Scanning @PluggableMessageHandlers-annotated classes");
        String[] basePackages = ScannerHelper.findBasePackagesOnClasspath(applicationContext.getClassLoader());
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

        logger.debug("Scanning the following base packages: {}", (Object) basePackages);

        for (String basePackage : basePackages) {
            configurationBuilder.addUrls(ClasspathHelper.forPackage(basePackage));
        }

        Reflections reflections = new Reflections(configurationBuilder);

        Set<Class<?>> handlerClasses = reflections.getTypesAnnotatedWith(PluggableMessageHandlers.class);

        logger.info("Found {} classes annotated with @PluggableMessageHandlers", handlerClasses.size());
        if (logger.isDebugEnabled()) {
            logger.debug(
                "Found the following classes annotated with @PluggableMessageHandlers: {}",
                handlerClasses.stream().map(Class::getName).collect(Collectors.toList())
            );
        }

        for (Class<?> handlerClass : handlerClasses) {
            PluggableMessageHandlers handlerAnnotation = handlerClass.getAnnotation(PluggableMessageHandlers.class);
            logger.debug(
                "Registering @PluggableMessageHandlers class [{}] for Method Actor class [{}]",
                handlerClass.getName(),
                handlerAnnotation.value().getName()
            );
            registry.put(handlerAnnotation.value(),handlerClass);
        }
    }

    @Override
    public List<Class<?>> getMessageHandlers(Class<? extends MethodActor> methodActor) {
        return registry.get(methodActor);
    }
}
