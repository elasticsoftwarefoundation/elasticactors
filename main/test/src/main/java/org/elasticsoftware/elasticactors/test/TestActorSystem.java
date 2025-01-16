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

package org.elasticsoftware.elasticactors.test;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ServiceActor;
import org.elasticsoftware.elasticactors.client.cluster.RemoteActorSystems;
import org.elasticsoftware.elasticactors.runtime.ScannerHelper;
import org.elasticsoftware.elasticactors.spring.ActorAnnotationBeanNameGenerator;
import org.elasticsoftware.elasticactors.spring.AnnotationConfigApplicationContext;
import org.elasticsoftware.elasticactors.test.configuration.TestConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.stereotype.Component;

/**
 * @author Joost van de Wijgerd
 */
public final class TestActorSystem {

    private final static Logger logger = LoggerFactory.getLogger(TestActorSystem.class);

    //public static final String CONFIGURATION_BASEPACKAGE = "org.elasticsoftware.elasticactors.test.configuration";

    private final boolean ignoreDefaultConfigClass;

    private AnnotationConfigApplicationContext applicationContext;
    private ActorSystem actorSystem;
    private RemoteActorSystems remoteActorSystems;

    private final Class customConfigurationClass;

    public TestActorSystem() {
        this(null);
    }

    public TestActorSystem(Class customConfigurationClass) {
        this(customConfigurationClass, false);
    }

    public TestActorSystem(Class customConfigurationClass, boolean ignoreDefaultConfigClass) {
        this.customConfigurationClass = customConfigurationClass;
        this.ignoreDefaultConfigClass = ignoreDefaultConfigClass;
    }

    public ActorSystem getActorSystem() {
        if (actorSystem == null) {
            actorSystem = applicationContext.getBean("internalActorSystem", ActorSystem.class);
        }
        return actorSystem;
    }

    public ActorSystem getActorSystem(String name) {
        return getActorSystem();
    }

    public ActorSystem getRemoteActorSystem() {
        if (remoteActorSystems == null) {
            remoteActorSystems = applicationContext.getBean(RemoteActorSystems.class);
        }
        return remoteActorSystems.get("testCluster", "test");
    }

    @PostConstruct
    public void initialize() {
        logger.info("Starting up Test Actor System");
        // annotation configuration context
        applicationContext = new AnnotationConfigApplicationContext();
        if (!ignoreDefaultConfigClass) {
            // set the correct configurations
            applicationContext.register(TestConfiguration.class);
        }
        // add custom configuration class
        if(customConfigurationClass != null) {
            applicationContext.register(customConfigurationClass);
        }
        //applicationContext.
        // ensure the EA annotations are scanned
        applicationContext.addIncludeFilters(new AnnotationTypeFilter(ServiceActor.class));
        // and exclude the spring ones
        applicationContext.addExcludeFilters(new AnnotationTypeFilter(Component.class));
        // generate correct names for ServiceActor annotated actors
        applicationContext.setBeanNameGenerator(new ActorAnnotationBeanNameGenerator());
        // find all the paths to scan
        applicationContext.scan(ScannerHelper.findBasePackagesOnClasspath(/*CONFIGURATION_BASEPACKAGE*/));
        // load em up
        applicationContext.refresh();
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread("SHUTDOWN-HOOK") {
            @Override
            public void run() {
                TestActorSystem.this.destroy();
            }
        }
        );
    }

    @PreDestroy
    public void destroy() {
        try {
            logger.info("Shutting down Test Actor System");
            // give the system some time to clean up
            Thread.sleep(1000);
        } catch(InterruptedException e) {
            // ignore
        } finally {
            applicationContext.close();
        }
    }
}
