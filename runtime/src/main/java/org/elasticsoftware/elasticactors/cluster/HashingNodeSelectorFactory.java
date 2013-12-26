package org.elasticsoftware.elasticactors.cluster;

import org.elasticsoftware.elasticactors.PhysicalNode;

import java.util.List;

/**
 * @author Joost van de Wijgerd
 */
public final class HashingNodeSelectorFactory implements NodeSelectorFactory {
    @Override
    public NodeSelector create(List<PhysicalNode> nodes) {
        return new HashingNodeSelector(nodes);
    }

    @Override
    public void start() throws Exception {
        // nothing to do
    }
}
