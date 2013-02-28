/*
 * Copyright (c) 2013 Joost van de Wijgerd <jwijgerd@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasterix.elasticactors;

import org.elasterix.elasticactors.serialization.Deserializer;
import org.elasterix.elasticactors.serialization.Serializer;

/**
 * @author Joost van de Wijgerd
 */
public interface ActorSystem<I> {
    String getName();

    int getNumberOfVirtualNodes();

    ActorRef createActor(I actorId,Class<?> actorClass);

    Serializer getSerializer(Class<?> messageClass);

    Deserializer getDeserializer(Class<?> messageClass);
}
