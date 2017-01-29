/*
 * Copyright 2013 - 2017 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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