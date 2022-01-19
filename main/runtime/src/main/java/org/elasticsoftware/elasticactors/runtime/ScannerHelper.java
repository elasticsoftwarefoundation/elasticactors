/*
 *   Copyright 2013 - 2022 The Original Authors
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @author Joost van de Wijgerd
 */
public final class ScannerHelper {
    private static final Logger logger = LoggerFactory.getLogger(ScannerHelper.class);
    public static final String RESOURCE_NAME = "META-INF/elasticactors.properties";

    public static String[] findBasePackagesOnClasspath(String... defaultPackages) {
        return findBasePackagesOnClasspath(Thread.currentThread().getContextClassLoader(),defaultPackages);
    }

    public static String[] findBasePackagesOnClasspath(ClassLoader classLoader,String... defaultPackages) {
        // scan everything for META-INF/elasticactors.properties
        // add the core configuration package
        Set<String> basePackages = new HashSet<>(Arrays.asList(defaultPackages));

        try {
            Enumeration<URL> resources = classLoader.getResources(RESOURCE_NAME);
            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();
                Properties props = new Properties();
                props.load(url.openStream());
                basePackages.add(props.getProperty("basePackage"));
            }
        } catch(IOException e) {
            logger.warn("Failed to load elasticactors.properties",e);
        }
        return basePackages.toArray(new String[0]);
    }
}
