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

package org.elasticsoftware.elasticactors.spring;

import org.elasticsoftware.elasticactors.ServiceActor;
import org.elasticsoftware.elasticactors.runtime.ScannerHelper;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.core.type.filter.AnnotationTypeFilter;

/**
 * @author Joost van de Wijgerd
 */
public final class ActorSystemApplicationContextInitializer implements ApplicationContextInitializer<AnnotationConfigWebApplicationContext> {
    @Override
    public void initialize(AnnotationConfigWebApplicationContext applicationContext) {
        // ensure the EA annotations are scanned
        applicationContext.addIncludeFilters(new AnnotationTypeFilter(ServiceActor.class));
        // generate correct names for ServiceActor annotated actors
        applicationContext.setBeanNameGenerator(new ActorAnnotationBeanNameGenerator());
        // add all the elastic actors base packages to the scan configuration
        applicationContext.scan(ScannerHelper.findBasePackagesOnClasspath(applicationContext.getClassLoader()));
    }
}
