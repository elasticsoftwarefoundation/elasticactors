package org.elasticsoftware.elasticactors.configuration;

import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.kubernetes.cluster.KubernetesClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

public class ClusteringConfiguration {
    @Autowired
    private Environment env;

    @Bean(name= "clusterService")
    public ClusterService createClusterService() {
        String namespace = env.getProperty("ea.cluster.kubernetes.namespace", "default");
        String name = env.getRequiredProperty("ea.cluster.kubernetes.statefulsetName");
        String nodeId = env.getRequiredProperty("ea.node.id");
        return new KubernetesClusterService(namespace, name, nodeId);
    }
}
