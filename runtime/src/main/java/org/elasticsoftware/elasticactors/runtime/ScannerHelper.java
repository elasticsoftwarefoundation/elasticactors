package org.elasticsoftware.elasticactors.runtime;

import org.apache.log4j.Logger;
import org.reflections.util.ClasspathHelper;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.*;

/**
 * @author Joost van de Wijgerd
 */
public final class ScannerHelper {
    private static final Logger logger = Logger.getLogger(ScannerHelper.class);
    public static final String RESOURCE_NAME = "META-INF/elasticactors.properties";

    public static String[] findBasePackagesOnClasspath(String... defaultPackages) {
        return findBasePackagesOnClasspath(Thread.currentThread().getContextClassLoader(),defaultPackages);
    }

    public static String[] findBasePackagesOnClasspath(ClassLoader classLoader,String... defaultPackages) {
        // scan everything for META-INF/elasticactors.properties
        Set<String> basePackages = new HashSet<>();
        // add the core configuration package
        basePackages.addAll(Arrays.asList(defaultPackages));

        try {
            Enumeration<URL> resources = classLoader.getResources(RESOURCE_NAME);
            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();
                Properties props = new Properties();
                props.load(url.openStream());
                basePackages.add(props.getProperty("basePackage"));
            }
        } catch(IOException e) {
            logger.warn(String.format("Failed to load elasticactors.properties"),e);
        }
        return basePackages.toArray(new String[basePackages.size()]);
    }
}
