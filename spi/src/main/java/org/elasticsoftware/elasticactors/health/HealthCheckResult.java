package org.elasticsoftware.elasticactors.health;

/**
 * @author Rob de Boer
 */
public class HealthCheckResult {

    private static final HealthCheckResult HEALTHY = new HealthCheckResult(true, null, null);

    private final boolean healthy;
    private final String message;
    private final Throwable error;

    public static HealthCheckResult healthy() {
        return HEALTHY;
    }

    public HealthCheckResult(boolean healthy, String message, Throwable error) {
        this.healthy = healthy;
        this.message = message;
        this.error = error;
    }

    public static HealthCheckResult unhealthy(String message) {
        return new HealthCheckResult(false, message, null);
    }

    public static HealthCheckResult unhealthy(Throwable error) {
        return new HealthCheckResult(false, error.getMessage(), error);
    }

    public static HealthCheckResult unhealthy(String message, Throwable error) {
        return new HealthCheckResult(false, message, error);
    }

    public boolean isHealthy() {
        return this.healthy;
    }

    public String getMessage() {
        return this.message;
    }

    public Throwable getError() {
        return this.error;
    }
}
