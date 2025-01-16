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

package org.elasticsoftware.elasticactors.indexing.elasticsearch;

import org.elasticsoftware.elasticactors.state.ActorLifecycleStep;

import java.lang.annotation.*;

import static org.elasticsoftware.elasticactors.state.ActorLifecycleStep.ACTIVATE;
import static org.elasticsoftware.elasticactors.state.ActorLifecycleStep.CREATE;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface IndexConfig {

    /**
     * Provide a list of message classes (i.e. objects annotated with {@link org.elasticsoftware.elasticactors.serialization.Message})
     * that should trigger the state if this actor to be re-indexed in elasticsearch
     *
     *
     * @return
     */
    Class<?>[] includedMessages() default {};

    /**
     * Used to control the persistence behavior after a {@link ActorLifecycleStep} has been fired. By default the state
     * will be saved after {@link ActorLifecycleStep#CREATE} and {@link ActorLifecycleStep#ACTIVATE}
     *
     * @see org.elasticsoftware.elasticactors.ElasticActor#postCreate(org.elasticsoftware.elasticactors.ActorRef)
     * @see org.elasticsoftware.elasticactors.ElasticActor#postActivate(String)
     * @see org.elasticsoftware.elasticactors.ElasticActor#prePassivate()
     * @see org.elasticsoftware.elasticactors.ElasticActor#preDestroy(org.elasticsoftware.elasticactors.ActorRef)  
     * @return
     */
    ActorLifecycleStep[] indexOn() default {CREATE, ACTIVATE};

    /**
     * Base index name for this actor. This should be unique per installation.
     */
    String indexName();

    /**
     * Type name for this actor
     */
    String typeName();

    /**
     *
     * How to version the actor states saved in elasticsearch. By default no versioning is applied
     * and all actors are saved in the same index.
     *
     *
     */
    VersioningStrategy versioningStrategy() default VersioningStrategy.NONE;

    /**
     * NONE: no versioning applied
     * REINDEX_ON_ACTIVATE: when the actor is activated for the first time, the old versions of this actor
     * are removed from the indices.
     *
     */
    enum VersioningStrategy {
        NONE,
        REINDEX_ON_ACTIVATE
    }

}
