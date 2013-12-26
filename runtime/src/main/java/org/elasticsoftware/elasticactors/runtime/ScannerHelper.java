package org.elasticsoftware.elasticactors.runtime;

import org.apache.log4j.Logger;
import org.reflections.util.ClasspathHelper;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @author Joost van de Wijgerd
 */
public final class ScannerHelper {
    private static final Logger logger = Logger.getLogger(ScannerHelper.class);
    public static final String RESOURCE_NAME = "META-INF/elasticactors.properties";

    public static String[] findBasePackages(String... defaultPackages) {
        // scan everything for META-INF/elasticactors.properties
        Set<String> basePackages = new HashSet<>();
        // add the core configuration package
        basePackages.addAll(Arrays.asList(defaultPackages));
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
}
