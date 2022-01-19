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

package org.elasticsoftware.elasticactors.runtime;

import com.google.common.collect.ImmutableList;
import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.ManagedActor;
import org.elasticsoftware.elasticactors.ManagedActorsRegistry;
import org.elasticsoftware.elasticactors.ServiceActor;
import org.elasticsoftware.elasticactors.SingletonActor;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import java.util.stream.Collectors;

/**
 * Find all classes annotated with {@link SingletonActor} and {@link ManagedActor}
 */
public final class ManagedActorsScanner implements ManagedActorsRegistry {

    private final static Logger logger = LoggerFactory.getLogger(ManagedActorsScanner.class);

    private final ApplicationContext applicationContext;

    public ManagedActorsScanner(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    private ImmutableList<Class<? extends ElasticActor<?>>> singletonActorClasses;
    private ImmutableList<Class<? extends ElasticActor<?>>> managedActorClasses;

    @Override
    @PostConstruct
    public synchronized void init() {
        logger.info("Scanning Managed Actor classes");
        String[] basePackages = ScannerHelper.findBasePackagesOnClasspath(applicationContext.getClassLoader());
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

        logger.debug("Scanning the following base packages: {}", (Object) basePackages);

        for (String basePackage : basePackages) {
            configurationBuilder.addUrls(ClasspathHelper.forPackage(basePackage));
        }

        Reflections reflections = new Reflections(configurationBuilder);

        logger.info("Scanning @SingletonActor-annotated classes");
        this.singletonActorClasses = reflections.getTypesAnnotatedWith(SingletonActor.class)
                .stream()
                .filter(ElasticActor.class::isAssignableFrom)
                .filter(c -> c.isAnnotationPresent(Actor.class))
                .filter(c -> !c.isAnnotationPresent(ManagedActor.class))
                .filter(c -> !c.isAnnotationPresent(ServiceActor.class))
                .map(c -> (Class<? extends ElasticActor<?>>) c)
                .collect(ImmutableList.toImmutableList());
        logger.info("Found {} classes annotated with @SingletonActor", singletonActorClasses.size());
        if (logger.isDebugEnabled()) {
            logger.debug(
                "Found the following classes annotated with @SingletonActor: {}",
                singletonActorClasses.stream().map(Class::getName).collect(Collectors.toList())
            );
        }

        logger.info("Scanning @ManagedActor-annotated classes");
        this.managedActorClasses = reflections.getTypesAnnotatedWith(ManagedActor.class)
                .stream()
                .filter(ElasticActor.class::isAssignableFrom)
                .filter(c -> c.isAnnotationPresent(Actor.class))
                .filter(c -> !c.isAnnotationPresent(SingletonActor.class))
                .filter(c -> !c.isAnnotationPresent(ServiceActor.class))
                .map(c -> (Class<? extends ElasticActor<?>>) c)
                .collect(ImmutableList.toImmutableList());
        logger.info("Found {} classes annotated with @ManagedActor", managedActorClasses.size());
        if (logger.isDebugEnabled()) {
            logger.debug(
                "Found the following classes annotated with @ManagedActor: {}",
                managedActorClasses.stream().map(Class::getName).collect(Collectors.toList())
            );
        }
    }

    @Override
    public ImmutableList<Class<? extends ElasticActor<?>>> getSingletonActorClasses() {
        return singletonActorClasses;
    }

    @Override
    public ImmutableList<Class<? extends ElasticActor<?>>> getManagedActorClasses() {
        return managedActorClasses;
    }
}
