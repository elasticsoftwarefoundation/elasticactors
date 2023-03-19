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

package org.elasticsoftware.elasticactors.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ServiceActor;
import org.elasticsoftware.elasticactors.configuration.ClusteringConfiguration;
import org.elasticsoftware.elasticactors.spring.ActorAnnotationBeanNameGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.aspectj.EnableSpringConfigured;
import org.springframework.core.env.Environment;

import jakarta.annotation.PostConstruct;

/**
 * @author Joost van de Wijgerd
 */
@Configuration
@EnableSpringConfigured
@EnableMBeanExport
@PropertySource(value = {"classpath:system.properties", "classpath:application.properties"}, ignoreResourceNotFound = true )
@ComponentScan(
        basePackages = {"org.elasticsoftware.elasticactors.base.serialization","org.elasticsoftware.elasticactors.kafka"},
        nameGenerator = ActorAnnotationBeanNameGenerator.class,
        includeFilters = {@ComponentScan.Filter(value = {ServiceActor.class}, type = FilterType.ANNOTATION)})
@Import(value = {ClusteringConfiguration.class, NodeConfiguration.class})
public class ContainerConfiguration {
    private Environment environment;
    private ActorSystem actorSystem;
    private ObjectMapper objectMapper;

    @Autowired
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Autowired
    public void setActorSystem(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    @Autowired
    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() throws Exception {
    }

}
