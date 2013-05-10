/*
 * Copyright 2013 Joost van de Wijgerd
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

/**
 * @author Joost van de Wijgerd
 */
public interface ActorSystemBootstrapper {
    /**
     * Called when the ActorSystem is initialized. This will happen always when the instance is created. Can be used
     * to hook into internal structures (like {@link org.elasticsoftware.elasticactors.cluster.ActorRefFactory}) needed to
     * implement serializers.
     *
     * @param actorSystem
     * @throws Exception
     */
    void initialize(ActorSystem actorSystem) throws Exception;

    /**
     * Called when an {@link ActorSystem} instance is created, gives the ability to create default actors etc
     *
     * @param actorSystem
     * @param arguments
     * @throws Exception
     */
    void create(ActorSystem actorSystem, String... arguments) throws Exception;

    /**
     * Called when an {@link ActorSystem} instance is activated (most of the times after a restart of a node)
     *
     * @param actorSystem
     * @throws Exception
     */
    void activate(ActorSystem actorSystem) throws Exception;

    void destroy() throws Exception;
}
