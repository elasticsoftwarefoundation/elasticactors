/*
 * Copyright 2013 - 2023 The Original Authors
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

package org.elasticsoftware.elasticactors.configuration;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CassandraSessionManager {

    private final CqlSession cassandraSession;

    public CassandraSessionManager(Environment env) {
        String cassandraHosts = env.getProperty("ea.cassandra.hosts","localhost:9042");
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

        List<InetSocketAddress> contactPointAddresses = Arrays.stream(contactPoints)
            .map(host -> new InetSocketAddress(host, cassandraPort))
            .collect(Collectors.toList());

        DriverConfigLoader driverConfigLoader = DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, Duration.ofSeconds(60))
            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, ConsistencyLevel.QUORUM.name())
            .withString(DefaultDriverOption.RETRY_POLICY_CLASS, "DefaultRetryPolicy")
            .withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS, "ConstantReconnectionPolicy")
            .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofSeconds(env.getProperty("ea.cassandra.retryDownedHostsDelayInSeconds",Integer.class,1)))
            .withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, "DcInferringLoadBalancingPolicy")
            .build();


        cassandraSession = CqlSession.builder()
            .addContactPoints(contactPointAddresses)
            .withConfigLoader(driverConfigLoader)
            .withKeyspace(cassandraKeyspaceName)
            .build();
    }

    public CqlSession getSession() {
        return cassandraSession;
    }

    @PreDestroy
    public void destroy() {
        this.cassandraSession.close();
    }

}
