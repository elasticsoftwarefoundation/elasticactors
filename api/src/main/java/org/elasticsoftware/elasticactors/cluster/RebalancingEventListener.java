package org.elasticsoftware.elasticactors.cluster;

/**
 * Listener that can tap in to Rebalancing events. When the cluster topology changes the ElasticActors framework will
 * start to rebalance. By using implementing this listener, and calling
 * {@link org.elasticsoftware.elasticactors.ActorSystems#registerRebalancingEventListener(RebalancingEventListener)}
 * a Actor will be able to get rebalancing events.
 *
 * This is mainly useful for {@link org.elasticsoftware.elasticactors.ServiceActor}s and should be used with care on
 * normal {@link org.elasticsoftware.elasticactors.Actor}s as it imposes overhead on the rebalancing logic it is
 * running on the same thread that handles the rebalancing itself
 *
 * @author Joost van de Wijgerd
 */
public interface RebalancingEventListener {
    /**
     * This method is called before the scale up sequence starts
     */
    void preScaleUp();

    /**
     * This method is called after the scale up sequence finishes
     */
    void postScaleUp();

    /**
     * This method is called before the scale down sequence starts
     */
    void preScaleDown();

    /**
     * this method is called after the scale down sequence finishes
     */
    void postScaleDown();
}
