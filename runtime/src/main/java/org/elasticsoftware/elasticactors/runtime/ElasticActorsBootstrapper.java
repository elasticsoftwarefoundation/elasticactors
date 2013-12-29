package org.elasticsoftware.elasticactors.runtime;

import org.apache.log4j.BasicConfigurator;
import org.elasticsoftware.elasticactors.ServiceActor;
import org.elasticsoftware.elasticactors.spring.AnnotationConfigApplicationContext;
import org.springframework.core.type.filter.AnnotationTypeFilter;

/**
 * @author Joost van de Wijgerd
 */
public final class ElasticActorsBootstrapper {


    public static final String CONFIGURATION_BASEPACKAGE = "org.elasticsoftware.elasticactors.configuration";

    private AnnotationConfigApplicationContext applicationContext;

    public static void main(String... args) {
        BasicConfigurator.configure();
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
        // find all the paths to scan
        applicationContext.scan(ScannerHelper.findBasePackagesOnClasspath(CONFIGURATION_BASEPACKAGE));
        // load em up
        applicationContext.refresh();
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread("SHUTDOWN-HOOK") {
            @Override
            public void run() {
                applicationContext.destroy();
            }
        }
        );
    }

    public ElasticActorsNode getNode() {
        return applicationContext.getBean(ElasticActorsNode.class);
    }




}
