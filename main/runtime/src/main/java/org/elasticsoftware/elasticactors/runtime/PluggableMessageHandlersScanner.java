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

package org.elasticsoftware.elasticactors.runtime;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.elasticsoftware.elasticactors.ActorLifecycleListener;
import org.elasticsoftware.elasticactors.ActorLifecycleListenerRegistry;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.MessageHandlersRegistry;
import org.elasticsoftware.elasticactors.MethodActor;
import org.elasticsoftware.elasticactors.PluggableMessageHandlers;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import javax.inject.Named;
import java.util.List;
import java.util.Set;

/**
 * Find all classes annotated with {@link PluggableMessageHandlers} and add them to the registry
 *
 * @author Joost van de Wijgerd
 */
@Named
public final class PluggableMessageHandlersScanner implements MessageHandlersRegistry, ActorLifecycleListenerRegistry {
    private static final Logger logger = LoggerFactory.getLogger(PluggableMessageHandlersScanner.class);
    private final ApplicationContext applicationContext;
    private final ListMultimap<Class<? extends MethodActor>,Class<?>> registry = LinkedListMultimap.create();
    private final ListMultimap<Class<? extends ElasticActor>,ActorLifecycleListener<?>> lifecycleListeners = LinkedListMultimap.create();

    public PluggableMessageHandlersScanner(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
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

        Set<Class<? extends ActorLifecycleListener>> listenerClasses = reflections.getSubTypesOf(ActorLifecycleListener.class);
        for (Class<? extends ActorLifecycleListener> listenerClass : listenerClasses) {
            try {
                ActorLifecycleListener lifeCycleListener = listenerClass.newInstance();
                // ensure that the lifeCycle listener handles the correct state class
                lifecycleListeners.put(lifeCycleListener.getActorClass(), lifeCycleListener);
            } catch(Exception e) {
                logger.error("Exception while instantiating ActorLifeCycleListener",e);
            }
        }
    }

    @Override
    public List<Class<?>> getMessageHandlers(Class<? extends MethodActor> methodActor) {
        return registry.get(methodActor);
    }

    @Override
    public List<ActorLifecycleListener<?>> getListeners(Class<? extends ElasticActor> actorClass) {
        return lifecycleListeners.get(actorClass);
    }


}
