/*
 *   Copyright 2013 - 2022 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.configuration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

import javax.annotation.PreDestroy;
import java.util.Set;

public class CassandraSessionManager {

    private final Session cassandraSession;

    public CassandraSessionManager(Environment env) {
        String cassandraHosts = env.getProperty("ea.cassandra.hosts","localhost:9042");
        String cassandraClusterName = env.getProperty("ea.cassandra.cluster","ElasticActorsCluster");
        String cassandraKeyspaceName = env.getProperty("ea.cassandra.keyspace","\"ElasticActors\"");
        Integer cassandraPort = env.getProperty("ea.cassandra.port", Integer.class, 9042);

        Set<String> hostSet = StringUtils.commaDelimitedListToSet(cassandraHosts);

        String[] contactPoints = new String[hostSet.size()];
        int i=0;
        for (String host : hostSet) {
            if(host.contains(":")) {
                contactPoints[i] = host.substring(0,host.indexOf(":"));
            } else {
                contactPoints[i] = host;
            }
            i+=1;
        }

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setHeartbeatIntervalSeconds(60);
        poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 2, env.getProperty("ea.cassandra.maxActive",Integer.class,Runtime.getRuntime().availableProcessors() * 3));
        poolingOptions.setPoolTimeoutMillis(2000);

        Cluster cassandraCluster =
            Cluster.builder().withClusterName(cassandraClusterName)
                .addContactPoints(contactPoints)
                .withPort(cassandraPort)
                .withLoadBalancingPolicy(new RoundRobinPolicy())
                .withRetryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE))
                .withPoolingOptions(poolingOptions)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(env.getProperty("ea.cassandra.retryDownedHostsDelayInSeconds",Integer.class,1) * 1000))
                .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM)).build();

        this.cassandraSession = cassandraCluster.connect(cassandraKeyspaceName);
    }

    public Session getSession() {
        return cassandraSession;
    }

    @PreDestroy
    public void destroy() {
        this.cassandraSession.close();
        this.cassandraSession.getCluster().close();
    }

}
