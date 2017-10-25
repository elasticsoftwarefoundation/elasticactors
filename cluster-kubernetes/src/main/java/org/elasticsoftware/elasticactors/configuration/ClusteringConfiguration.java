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
        return new KubernetesClusterService("");
    }
}
