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

package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.ActorRef;

import java.io.IOException;

/**
 * @author Joost van de Wijgerd
 */
public interface ActorSystemEventListenerRegistry {
    /**
     *
     *
     * @param   receiver    An {@link org.elasticsoftware.elasticactors.ActorRef} to a Persistent Actor (i.e. annotated with {@link org.elasticsoftware.elasticactors.Actor})
     * @param   event       The type of event to subscribe to
     * @param   message     The message to send to the receiver when the event is triggered. Needs to be annotated with {@link org.elasticsoftware.elasticactors.serialization.Message})
     * @throws java.lang.IllegalStateException when this method ios not called in the context of an actor
     */
    public void register(ActorRef receiver, ActorSystemEvent event, Object message) throws IOException;

    /**
     * Deregisters the activation hook (if any) for the calling actor. Only works while being called in the context of
     * a persistent actor (i.e. annotated with the {@link org.elasticsoftware.elasticactors.Actor} annotation).
     *
     * If there is currently no activation hook registered this method will silently fail.
     *
     * @throws java.lang.IllegalStateException when this method not called in the context of an actor
     */
    public void deregister(ActorRef receiver, ActorSystemEvent event);
}
