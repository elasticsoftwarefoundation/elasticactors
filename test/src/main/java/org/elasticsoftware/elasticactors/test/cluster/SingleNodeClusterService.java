package org.elasticsoftware.elasticactors.test.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ClusterEventListener;
import org.elasticsoftware.elasticactors.cluster.ClusterMessageHandler;
import org.elasticsoftware.elasticactors.cluster.ClusterService;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Joost van de Wijgerd
 */
public class SingleNodeClusterService implements ClusterService {
    private static Logger logger = LogManager.getLogger(SingleNodeClusterService.class);
    private final Queue<ClusterEventListener> eventListeners = new ConcurrentLinkedQueue<>();
    private ClusterMessageHandler clusterMessageHandler;
    private final PhysicalNode localNode;

    public SingleNodeClusterService(PhysicalNode localNode) {
        this.localNode = localNode;
    }

    @Override
    public void reportReady() {
        for (ClusterEventListener eventListener : eventListeners) {
            //eventListener.onTopologyChanged(Arrays.asList(localNode));
            try {
                eventListener.onMasterElected(localNode);
            } catch (Exception e) {
                logger.error("Exception in onMasterElected",e);
            }
        }
    }

    @Override
    public void reportPlannedShutdown() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void addEventListener(ClusterEventListener eventListener) {
        eventListeners.add(eventListener);
    }

    @Override
    public void sendMessage(String memberToken, byte[] message) throws Exception {
        //@todo: send to local?
    }

    @Override
    public void setClusterMessageHandler(ClusterMessageHandler clusterMessageHandler) {
        this.clusterMessageHandler = clusterMessageHandler;
    }
}
