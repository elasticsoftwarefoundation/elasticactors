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

package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ServiceActor;
import org.elasticsoftware.elasticactors.runtime.ScannerHelper;
import org.elasticsoftware.elasticactors.spring.ActorAnnotationBeanNameGenerator;
import org.elasticsoftware.elasticactors.spring.AnnotationConfigApplicationContext;
import org.elasticsoftware.elasticactors.test.configuration.TestConfiguration;
import org.springframework.core.env.Environment;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Properties;

/**
 * @author Joost van de Wijgerd
 */
public final class TestActorSystem {
    //public static final String CONFIGURATION_BASEPACKAGE = "org.elasticsoftware.elasticactors.test.configuration";

    private AnnotationConfigApplicationContext applicationContext;

    public TestActorSystem() {

    }

    public ActorSystem getActorSystem() {
        return applicationContext.getBean(ActorSystem.class);
    }

    public ActorSystem getActorSystem(String name) {
        return getActorSystem();
    }

    @PostConstruct
    public void initialize() {
        // annotation configuration context
        applicationContext = new AnnotationConfigApplicationContext();
        // set the correct configurations
        applicationContext.register(TestConfiguration.class);
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
        applicationContext.destroy();
    }
}
