/*
 * Copyright 2013 - 2014 The Original Authors
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

import java.util.ArrayList;
import java.util.List;

/**
 *
 * It represents the node of the graph. It holds a user value that is passed
 * back to the user when a node gets the chance to be evaluated.
 *
 * @author nicolae caralicea
 *
 * @param <T>
 */
final class GraphNode<T> {
    public T value;
    private List<GraphNode<T>> comingInNodes;
    private List<GraphNode<T>> goingOutNodes;

    /**
     * Adds an incoming node to the current node
     *
     * @param node
     *            The incoming node
     */
    public void addComingInNode(GraphNode<T> node) {
        if (comingInNodes == null)
            comingInNodes = new ArrayList<GraphNode<T>>();
        comingInNodes.add(node);
    }

    /**
     * Adds an outgoing node from the current node
     *
     * @param node
     *            The outgoing node
     */
    public void addGoingOutNode(GraphNode<T> node) {
        if (goingOutNodes == null)
            goingOutNodes = new ArrayList<GraphNode<T>>();
        goingOutNodes.add(node);
    }

    /**
     * Provides all the coming in nodes
     *
     * @return The coming in nodes
     */
    public List<GraphNode<T>> getComingInNodes() {
        return comingInNodes;
    }

    /**
     * Provides all the going out nodes
     *
     * @return The going out nodes
     */
    public List<GraphNode<T>> getGoingOutNodes() {
        return goingOutNodes;
    }
}
