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

package org.elasticsoftware.elasticactors.cluster.tasks;

import org.elasticsoftware.elasticactors.ActorContext;
import org.elasticsoftware.elasticactors.ActorContextHolder;
import org.elasticsoftware.elasticactors.reactivestreams.ProcessorContext;
import org.elasticsoftware.elasticactors.state.PersistentActor;

/**
 * @author Joost van de Wijgerd
 */
public final class InternalActorContext extends ActorContextHolder {

    private InternalActorContext() {
        super();
    }

    protected static ActorContext setContext(ActorContext context) {
        final ActorContext currentContext = threadContext.get();
        threadContext.set(context);
        return currentContext;
    }

    protected static ActorContext getAndClearContext() {
        ActorContext state = threadContext.get();
        threadContext.set(null);
        return state;
    }

    public static ProcessorContext getAsProcessorContext() {
        return (ProcessorContext) threadContext.get();
    }
}
