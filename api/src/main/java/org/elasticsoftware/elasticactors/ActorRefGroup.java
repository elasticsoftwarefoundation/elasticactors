/*
 * Copyright 2013 - 2016 The Original Authors
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
import java.util.Set;

/**
 * @author Joost van de Wijgerd
 */
public interface ActorRefGroup {
    /**
     * This is the main method to send a message to all {@link ElasticActor}s in the group.
     *
     * @param message       the message to send (needs to be annotated with {@link org.elasticsoftware.elasticactors.serialization.Message}
     * @param sender        the sender, this can be self, but it can also be another {@link ActorRef}
     * @throws              MessageDeliveryException when something is wrong with the Messaging Subsystem
     */
    void tell(Object message, ActorRef sender) throws MessageDeliveryException;

    /**
     * Equivalent to calling ActorRefGroup.tell(message,getSelf())
     *
     * @param message       the message to send (needs to be annotated with {@link org.elasticsoftware.elasticactors.serialization.Message}
     * @throws              IllegalStateException if the method is not called withing a {@link ElasticActor} lifecycle or on(Message) method
     * @throws              MessageDeliveryException when somthing is wrong with the Messaging Subsystem
     */
    void tell(Object message) throws IllegalStateException, MessageDeliveryException;

    Collection<? extends ActorRef> getMembers();

    ActorRefGroup withMembersAdded(ActorRef... membersToAdd);

    ActorRefGroup withMembersRemoved(ActorRef... membersToRemove);
}
