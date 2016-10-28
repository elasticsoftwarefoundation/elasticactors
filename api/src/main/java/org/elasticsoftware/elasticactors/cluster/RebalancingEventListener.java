/*
 * Copyright 2013 - 2016 The Original Authors
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
