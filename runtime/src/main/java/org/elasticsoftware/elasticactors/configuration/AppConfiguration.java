package org.elasticsoftware.elasticactors.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.aspectj.EnableSpringConfigured;

/**
 * @author Joost van de Wijgerd
 */
@Configuration
@EnableSpringConfigured
@PropertySource(value = "file:/etc/elasticactors/system.properties")
@Import(value = {NodeConfiguration.class,MessagingConfiguration.class,BackplaneConfiguration.class})
public class AppConfiguration {
}
