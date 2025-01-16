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

package org.elasticsoftware.elasticactors.kafka;

import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.ActorContextHolder;

/**
 * @author Joost van de Wijgerd
 */
final class InternalActorContext extends ActorContextHolder {

    private InternalActorContext() {
        super();
    }

    static void setContext(ActorContext context) {
        threadContext.set(context);
    }

    static void clearContext() {
        threadContext.remove();
    }

}
