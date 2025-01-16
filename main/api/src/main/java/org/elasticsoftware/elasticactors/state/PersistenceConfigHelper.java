/*
 * Copyright 2013 - 2025 The Original Authors
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

package org.elasticsoftware.elasticactors.state;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistenceConfigHelper {
    private PersistenceConfigHelper() {}

    public static boolean shouldUpdateState(PersistenceConfig persistenceConfig, Object message) {
        if (persistenceConfig != null) {
            // look for not excluded when persist all is on
            if(persistenceConfig.persistOnMessages()) {
                return !contains(persistenceConfig.excluded(), message.getClass());
            } else {
                // look for included otherwise
                return contains(persistenceConfig.included(), message.getClass());
            }
        } else {
            return true;
        }
    }

    private static <T> boolean contains(T[] array, T object) {
        for (T currentObject : array) {
            if (currentObject.equals(object)) {
                return true;
            }
        }
        return false;
    }

    public static boolean shouldUpdateState(PersistenceConfig persistenceConfig,ActorLifecycleStep lifecycleStep) {
        return persistenceConfig == null || contains(persistenceConfig.persistOn(), lifecycleStep);
    }
}
