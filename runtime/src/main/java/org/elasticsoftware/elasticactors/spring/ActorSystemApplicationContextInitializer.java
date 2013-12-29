package org.elasticsoftware.elasticactors.spring;

import org.elasticsoftware.elasticactors.runtime.ScannerHelper;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

/**
 * @author Joost van de Wijgerd
 */
public final class ActorSystemApplicationContextInitializer implements ApplicationContextInitializer<AnnotationConfigWebApplicationContext> {
    @Override
    public void initialize(AnnotationConfigWebApplicationContext applicationContext) {
        // add all the elastic actors base packages to the scan configuration
        applicationContext.scan(ScannerHelper.findBasePackagesOnClasspath(applicationContext.getClassLoader()));
    }
}
