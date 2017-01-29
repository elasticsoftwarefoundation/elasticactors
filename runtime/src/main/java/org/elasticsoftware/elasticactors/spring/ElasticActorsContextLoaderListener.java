/*
 * Copyright 2013 - 2017 The Original Authors
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

package org.elasticsoftware.elasticactors.spring;

import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.springframework.web.context.ContextLoaderListener;

import javax.servlet.ServletContextEvent;

/**
 * @author Joost van de Wijgerd
 */
public final class ElasticActorsContextLoaderListener extends ContextLoaderListener {
    @Override
    public void contextInitialized(ServletContextEvent event) {
        // make sure everything is initialized
        super.contextInitialized(event);
        // and signal ready state to the cluster
        ClusterService clusterService = getCurrentWebApplicationContext().getBean(ClusterService.class);
        try {
            clusterService.reportReady();
        } catch (Exception e) {
            throw new RuntimeException("Exception in ClusterService.reportReady()",e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        // first shut down the node
        //ElasticActorsNode node = getCurrentWebApplicationContext().getBean(ElasticActorsNode.class);
        // and then destroy the rest
        //node.reportPlannedShutdown();
        super.contextDestroyed(event);
    }
}
