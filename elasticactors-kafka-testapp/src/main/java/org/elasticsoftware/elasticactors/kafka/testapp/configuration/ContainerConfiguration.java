package org.elasticsoftware.elasticactors.kafka.testapp.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ServiceActor;
import org.elasticsoftware.elasticactors.configuration.ClusteringConfiguration;
import org.elasticsoftware.elasticactors.kafka.configuration.NodeConfiguration;
import org.elasticsoftware.elasticactors.spring.ActorAnnotationBeanNameGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import org.springframework.context.annotation.aspectj.EnableSpringConfigured;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;

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
