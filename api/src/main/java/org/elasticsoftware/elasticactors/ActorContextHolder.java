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

package org.elasticsoftware.elasticactors;

import java.util.Collection;

/**
 * Encapsulates a {@link ThreadLocal} that holds the {@link ActorContext} for the {@link ElasticActor} that
 * is currently being executed.
 *
 * @author Joost van de Wijgerd
 */
public class ActorContextHolder {
    protected static final ThreadLocal<ActorContext> threadContext = new ThreadLocal<>();

    protected ActorContextHolder() {

    }

    /**
     *
     * @param stateClass        the class that implements {@link ActorState} interface
     * @param <T>               generic type info
     * @return                  the current {@link ActorState} for the current executing {@link ElasticActor}
     */
    public static <T extends ActorState> T getState(Class<T> stateClass) {
        ActorContext actorContext = threadContext.get();
        return actorContext.getState(stateClass);
    }

    /**
     *
     * @return      the {@link ActorRef} to the current executing {@link ElasticActor}
     */
    public static ActorRef getSelf() {
        ActorContext actorContext =  threadContext.get();
        return actorContext != null ? actorContext.getSelf() : null;
    }

    /**
     *
     * @return  the {@link ActorSystem} that the current executing {@link ElasticActor} belongs to
     */
    public static ActorSystem getSystem() {
        return threadContext.get().getActorSystem();
    }

    public static Collection<PersistentSubscription> getSubscriptions() {
        return threadContext.get().getSubscriptions();
    }
}
