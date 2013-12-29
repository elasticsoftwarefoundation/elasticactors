package org.elasticsoftware.elasticactors.spring;

import org.elasticsoftware.elasticactors.runtime.ElasticActorsNode;
import org.springframework.web.context.ContextLoaderListener;

import javax.servlet.ServletContextEvent;

/**
 * @author Joost van de Wijgerd
 */
public final class ElasticActorsContextLoaderListener extends ContextLoaderListener {
    @Override
    public void contextInitialized(ServletContextEvent event) {
        // make sure everything is unitialied
        super.contextInitialized(event);
        // and signal ready state to the cluster
        ElasticActorsNode node = getCurrentWebApplicationContext().getBean(ElasticActorsNode.class);
        node.reportReady();
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
