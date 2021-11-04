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

import org.elasticsoftware.elasticactors.ServiceActor;
import org.elasticsoftware.elasticactors.spring.ActorAnnotationBeanNameGenerator;
import org.elasticsoftware.elasticactors.spring.AnnotationConfigApplicationContext;
import org.springframework.core.type.filter.AnnotationTypeFilter;

/**
 * @author Joost van de Wijgerd
 */
public final class ElasticActorsBootstrapper {


    public static final String CONFIGURATION_BASEPACKAGE = "org.elasticsoftware.elasticactors.configuration";

    private AnnotationConfigApplicationContext applicationContext;

    public static void main(String... args) throws Exception {
        ElasticActorsBootstrapper bootstrapper = new ElasticActorsBootstrapper();
        bootstrapper.init();
        bootstrapper.getNode().join();
    }

    public ElasticActorsBootstrapper() {
    }

    public void init() {
        // annotation configuration context
        applicationContext = new AnnotationConfigApplicationContext();
        // set the correct configurations
        // ensure the EA annotations are scanned
        applicationContext.addIncludeFilters(new AnnotationTypeFilter(ServiceActor.class));
        // generate correct names for ServiceActor annotated actors
        applicationContext.setBeanNameGenerator(new ActorAnnotationBeanNameGenerator());
        // find all the paths to scan
        applicationContext.scan(ScannerHelper.findBasePackagesOnClasspath(CONFIGURATION_BASEPACKAGE));
        // load em up
        applicationContext.refresh();
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread("SHUTDOWN-HOOK") {
            @Override
            public void run() {
                applicationContext.close();
            }
        }
        );
    }

    public ElasticActorsNode getNode() {
        return applicationContext.getBean(ElasticActorsNode.class);
    }




}
