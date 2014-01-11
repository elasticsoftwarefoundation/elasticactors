package org.elasticsoftware.elasticactors.test;

import org.elasticsoftware.elasticactors.ActorSystem;
import org.elasticsoftware.elasticactors.ServiceActor;
import org.elasticsoftware.elasticactors.runtime.ScannerHelper;
import org.elasticsoftware.elasticactors.spring.ActorAnnotationBeanNameGenerator;
import org.elasticsoftware.elasticactors.spring.AnnotationConfigApplicationContext;
import org.elasticsoftware.elasticactors.test.configuration.TestConfiguration;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author Joost van de Wijgerd
 */
public final class TestActorSystem {
    //public static final String CONFIGURATION_BASEPACKAGE = "org.elasticsoftware.elasticactors.test.configuration";

    private AnnotationConfigApplicationContext applicationContext;

    public TestActorSystem() {

    }

    public ActorSystem getActorSystem(String name) {
        return applicationContext.getBean(ActorSystem.class);
    }

    @PostConstruct
    public void initialize() {
        // annotation configuration context
        applicationContext = new AnnotationConfigApplicationContext();
        // set the correct configurations
        applicationContext.register(TestConfiguration.class);
        // ensure the EA annotations are scanned
        applicationContext.addIncludeFilters(new AnnotationTypeFilter(ServiceActor.class));
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
