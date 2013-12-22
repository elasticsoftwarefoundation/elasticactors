package org.elasticsoftware.elasticactors.runtime;

import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.reflections.util.ClasspathHelper;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

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
public class ElasticActorsNode implements PhysicalNode {
    private static final Logger logger = Logger.getLogger(ElasticActorsNode.class);
    public static final String RESOURCE_NAME = "META-INF/elasticactors.properties";
    public static final String CONFIGURATION_BASEPACKAGE = "org.elasticsoftware.elasticactors.configuration";
    private final String name;
    private final InetAddress address;
    private final CountDownLatch waitLatch = new CountDownLatch(1);
    private AnnotationConfigApplicationContext applicationContext;

    public static void main(String... args) {
        // @todo: generate name once in the config directory
        String name = args[0];
        ElasticActorsNode node = new ElasticActorsNode(name,null);
        node.init();
        node.join();
    }

    public ElasticActorsNode(String name, InetAddress address) {
        this.name = name;
        this.address = address;
    }

    public void init() {
        // annotation configuration context
        applicationContext = new AnnotationConfigApplicationContext();
        // set the correct configurations
        //applicationContext.register();
        // find all the paths to scan
        applicationContext.scan(findBasePackages());
        // load em up
        applicationContext.refresh();
    }

    public void join() {
        try {
            waitLatch.await();
        } catch (InterruptedException e) {
            //
        }
    }

    private String[] findBasePackages() {
        // scan everything for META-INF/elasticactors.properties
        Set<String> basePackages = new HashSet<>();
        // add the core configuration package
        basePackages.add(CONFIGURATION_BASEPACKAGE);
        Set<URL> resources = ClasspathHelper.forResource(RESOURCE_NAME);
        for (URL resource : resources) {
            try {
                Properties props = new Properties();
                props.load(new FileInputStream(new File(new URI(resource.toString()+RESOURCE_NAME))));
                basePackages.add(props.getProperty("basePackage"));
            } catch(Exception e) {
                logger.warn(String.format("Failed to load elasticactors.properties from URL: %s",resource.toString()),e);
            }
        }
        return basePackages.toArray(new String[basePackages.size()]);
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public String getId() {
        return name;
    }

    @Override
    public InetAddress getAddress() {
        return address;
    }
}
