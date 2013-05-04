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

package org.elasticsoftware.elasticactors.cluster.cassandra;

import org.apache.cassandra.tools.NodeProbe;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.NodeSelector;
import org.elasticsoftware.elasticactors.cluster.NodeSelectorFactory;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class CassandraNodeSelectorFactory implements NodeSelectorFactory {
    private NodeProbe nodeProbe;

    @Override
    public void start() throws Exception {
        nodeProbe = new NodeProbe("localhost");
    }

    @Override
    public NodeSelector create(List<PhysicalNode> nodes) {
        return new CassandraNodeSelector(nodeProbe,nodes);
    }
}
