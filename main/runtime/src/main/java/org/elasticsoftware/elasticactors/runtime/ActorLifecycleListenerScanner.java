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
import org.elasticsoftware.elasticactors.ActorLifecycleListener;
import org.elasticsoftware.elasticactors.ActorLifecycleListenerRegistry;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.PluggableMessageHandlers;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Find all classes annotated with {@link PluggableMessageHandlers} and add them to the registry
 *
 * @author Joost van de Wijgerd
 */
public final class ActorLifecycleListenerScanner implements ActorLifecycleListenerRegistry {
    private static final Logger logger = LoggerFactory.getLogger(ActorLifecycleListenerScanner.class);
    private final ApplicationContext applicationContext;
    private final ListMultimap<Class<? extends ElasticActor>,ActorLifecycleListener<?>> registry = LinkedListMultimap.create();

    public ActorLifecycleListenerScanner(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    @PostConstruct
    public synchronized void init() {
        logger.info("Scanning classes that implement ActorLifecycleListener");
        String[] basePackages = ScannerHelper.findBasePackagesOnClasspath(applicationContext.getClassLoader());
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

        logger.debug("Scanning the following base packages: {}", (Object) basePackages);

        for (String basePackage : basePackages) {
            configurationBuilder.addUrls(ClasspathHelper.forPackage(basePackage));
        }

        Reflections reflections = new Reflections(configurationBuilder);

        Set<Class<? extends ActorLifecycleListener>> listenerClasses = reflections.getSubTypesOf(ActorLifecycleListener.class);
        logger.info("Found {} classes that implement ActorLifecycleListener", listenerClasses.size());
        if (logger.isDebugEnabled()) {
            logger.debug(
                "Found the following classes implementing ActorLifecycleListener: {}",
                listenerClasses.stream().map(Class::getName).collect(Collectors.toList())
            );
        }
        for (Class<? extends ActorLifecycleListener> listenerClass : listenerClasses) {
            try {
                ActorLifecycleListener lifeCycleListener = listenerClass.newInstance();
                logger.debug(
                    "Registering instance of ActorLifecycleListener class [{}] for Actor class [{}]",
                    listenerClass.getName(),
                    lifeCycleListener.getActorClass().getName()
                );
                // ensure that the lifeCycle listener handles the correct state class
                registry.put(lifeCycleListener.getActorClass(), lifeCycleListener);
            } catch(Exception e) {
                logger.error("Exception while instantiating ActorLifeCycleListener",e);
            }
        }
    }

    @Override
    public List<ActorLifecycleListener<?>> getListeners(Class<? extends ElasticActor> actorClass) {
        return registry.get(actorClass);
    }


}
