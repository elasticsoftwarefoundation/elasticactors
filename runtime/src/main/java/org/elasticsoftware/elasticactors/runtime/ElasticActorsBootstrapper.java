package org.elasticsoftware.elasticactors.runtime;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.ServiceActor;
import org.reflections.util.ClasspathHelper;
import org.elasticsoftware.elasticactors.spring.AnnotationConfigApplicationContext;
import org.springframework.core.type.filter.AnnotationTypeFilter;


import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * @author Joost van de Wijgerd
 */
public class ElasticActorsBootstrapper {


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
        applicationContext.scan(ScannerHelper.findBasePackages(CONFIGURATION_BASEPACKAGE));
        // load em up
        applicationContext.refresh();
    }

    public ElasticActorsNode getNode() {
        return applicationContext.getBean(ElasticActorsNode.class);
    }




}
