package org.elasticsoftware.elasticactors.health;

import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.springframework.beans.factory.annotation.Autowired;

import static org.elasticsoftware.elasticactors.health.HealthCheckResult.healthy;
import static org.elasticsoftware.elasticactors.health.HealthCheckResult.unhealthy;

/**
 * @author Rob de Boer
 */
public class InternalActorSystemHealthCheck implements HealthCheck {

    private final InternalActorSystem internalActorSystem;

    @Autowired
    public InternalActorSystemHealthCheck(InternalActorSystem internalActorSystem) {
        this.internalActorSystem = internalActorSystem;
    }

    @Override
    public HealthCheckResult check() {
        if (internalActorSystem.isStable()) {
            return healthy();
        } else {
            return unhealthy("Actor system is not stable. Shards may not have been released or distributed properly");
        }
    }
}