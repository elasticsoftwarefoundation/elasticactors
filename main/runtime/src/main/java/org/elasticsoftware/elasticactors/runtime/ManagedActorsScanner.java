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

import org.elasticsoftware.elasticactors.Actor;
import org.elasticsoftware.elasticactors.ElasticActor;
import org.elasticsoftware.elasticactors.ManagedActor;
import org.elasticsoftware.elasticactors.ManagedActorsRegistry;
import org.elasticsoftware.elasticactors.ServiceActor;
import org.elasticsoftware.elasticactors.SingletonActor;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Find all classes annotated with {@link SingletonActor} and {@link ManagedActor}
 */
@Named
public final class ManagedActorsScanner implements ManagedActorsRegistry {

    @Inject
    private ApplicationContext applicationContext;

    private List<Class<? extends ElasticActor<?>>> singletonActorClasses;
    private List<Class<? extends ElasticActor<?>>> managedActorClasses;

    @Override
    @PostConstruct
    public void init() {
        String[] basePackages = ScannerHelper.findBasePackagesOnClasspath(applicationContext.getClassLoader());
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

        for (String basePackage : basePackages) {
            configurationBuilder.addUrls(ClasspathHelper.forPackage(basePackage));
        }

        Reflections reflections = new Reflections(configurationBuilder);

        this.singletonActorClasses = reflections.getTypesAnnotatedWith(SingletonActor.class)
                .stream()
                .filter(ElasticActor.class::isAssignableFrom)
                .filter(c -> c.isAnnotationPresent(Actor.class))
                .filter(c -> !c.isAnnotationPresent(ManagedActor.class))
                .filter(c -> !c.isAnnotationPresent(ServiceActor.class))
                .map(c -> (Class<? extends ElasticActor<?>>) c)
                .collect(Collectors.toList());

        this.managedActorClasses = reflections.getTypesAnnotatedWith(ManagedActor.class)
                .stream()
                .filter(ElasticActor.class::isAssignableFrom)
                .filter(c -> c.isAnnotationPresent(Actor.class))
                .filter(c -> !c.isAnnotationPresent(SingletonActor.class))
                .filter(c -> !c.isAnnotationPresent(ServiceActor.class))
                .map(c -> (Class<? extends ElasticActor<?>>) c)
                .collect(Collectors.toList());
    }

    @Override
    public List<Class<? extends ElasticActor<?>>> getSingletonActorClasses() {
        return singletonActorClasses;
    }

    @Override
    public List<Class<? extends ElasticActor<?>>> getManagedActorClasses() {
        return managedActorClasses;
    }
}
