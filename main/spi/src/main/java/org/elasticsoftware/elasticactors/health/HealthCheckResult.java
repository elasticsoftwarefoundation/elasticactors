/*
 * Copyright 2013 - 2024 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

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
