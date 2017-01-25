package org.elasticsoftware.elasticactors.health;

/**
 * @author Rob de Boer
 */
public interface HealthCheck {

    HealthCheckResult check();
}
