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

package org.elasticsoftware.elasticactors.kubernetes.cluster.statemachine.processor;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;

public interface StateProcessor {

    /**
     * Processes the current resource
     *
     * @param resource The current state of the Stateful Set
     * @return true if the resource should be processed by another processor after this one is done
     */
    boolean process(StatefulSet resource);
}
