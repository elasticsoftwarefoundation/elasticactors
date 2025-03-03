/*
 * Copyright 2013 - 2025 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.cassandra4.health;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import org.elasticsoftware.elasticactors.health.HealthCheck;
import org.elasticsoftware.elasticactors.health.HealthCheckResult;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsoftware.elasticactors.health.HealthCheckResult.healthy;
import static org.elasticsoftware.elasticactors.health.HealthCheckResult.unhealthy;

/**
 * @author Rob de Boer
 * @author Joost van de Wijgerd
 */
public class CassandraHealthCheck implements HealthCheck {

    private final CqlSession cassandraSession;

    @Autowired
    public CassandraHealthCheck(CqlSession cassandraSession) {
        this.cassandraSession = cassandraSession;
    }

    /**
     * Because we are doing quorum reads and writes the system is still healthy when there are N - 1 nodes up, where
     * N is the total number of nodes in the cluster.
     *
     * Check this tool: https://www.ecyrd.com/cassandracalculator/
     *
     * @return
     */
    @Override
    public HealthCheckResult check() {
        if (cassandraSession.isClosed()) {
            return unhealthy("Cassandra Session appears to be closed");
        }
        
        Set<Node> connectedHosts = cassandraSession.getMetadata().getNodes().values().stream()
                .filter(node -> node.getState().equals(NodeState.UP)).collect(Collectors.toSet());

        if(cassandraSession.getMetadata().getNodes().size() - connectedHosts.size() > 1) {
            return unhealthy("Not enough connected hosts to do QUORUM reads and writes");
        }

        /* This query caused cassandra machines to go oom on the production cluster
        try {
            String query = QueryBuilder.select().countAll().from("\"ElasticActors\"", "\"PersistentActors\"").getQueryString();
            ResultSet results = cassandraSession.execute(query);
            if (results.one() == null) {
                return unhealthy("No results found in Cassandra ElasticActors table");
            }
        } catch (Exception e) {
            return unhealthy("Unable to query Cassandra ElasticActors table: " + e.getMessage(), e);
        }
        */

        return healthy();
    }

}
