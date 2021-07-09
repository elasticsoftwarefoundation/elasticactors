/*
 *   Copyright 2013 - 2019 The Original Authors
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

package org.elasticsoftware.elasticactors.state;

import java.util.Arrays;

/**
 * @author Joost van de Wijgerd
 */
public final class PersistenceConfigHelper {
    private PersistenceConfigHelper() {}

    public static boolean shouldUpdateState(PersistenceConfig persistenceConfig, Object message) {
        if (persistenceConfig != null) {
            // look for not excluded when persist all is on
            if(persistenceConfig.persistOnMessages()) {
                return !Arrays.asList(persistenceConfig.excluded()).contains(message.getClass());
            } else {
                // look for included otherwise
                return Arrays.asList(persistenceConfig.included()).contains(message.getClass());
            }
        } else {
            return true;
        }
    }

    public static boolean shouldUpdateState(PersistenceConfig persistenceConfig,ActorLifecycleStep lifecycleStep) {
        return persistenceConfig == null || Arrays.asList(persistenceConfig.persistOn()).contains(lifecycleStep);
    }
}
