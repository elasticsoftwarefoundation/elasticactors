/*
 * Copyright 2013 the original authors
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

package org.elasticsoftware.elasticactors.dependencies;

/**
 * The main mechanism used for notifying the outside of the fact that a node
 * just got its evaluation
 *
 * @author nicolae caralicea
 *
 * @param <T>
 */
public interface NodeValueListener<T> {
    /**
     *
     * The callback method used to notify the fact that a node that has assigned
     * the nodeValue value just got its evaluation
     *
     * @param nodeValue
     *            The user set value of the node that just got the evaluation
     */
    void evaluating(T nodeValue);
}
