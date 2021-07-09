/*
 *   Copyright 2013 - 2019 The Original Authors
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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import org.elasticsoftware.elasticactors.cassandra.common.serialization.CompressingSerializer;
import org.elasticsoftware.elasticactors.cassandra.common.serialization.DecompressingDeserializer;
import org.elasticsoftware.elasticactors.cassandra4.cluster.CassandraActorSystemEventListenerRepository;
import org.elasticsoftware.elasticactors.cassandra4.cluster.scheduler.CassandraScheduledMessageRepository;
import org.elasticsoftware.elasticactors.cassandra4.health.CassandraHealthCheck;
import org.elasticsoftware.elasticactors.cassandra4.state.CassandraPersistentActorRepository;
import org.elasticsoftware.elasticactors.cassandra4.state.PersistentActorUpdateEventProcessor;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.ActorSystemEventListenerRepository;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRepository;
import org.elasticsoftware.elasticactors.serialization.Deserializer;
import org.elasticsoftware.elasticactors.serialization.Serializer;
import org.elasticsoftware.elasticactors.serialization.internal.ActorRefDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.PersistentActorDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.PersistentActorSerializer;
import org.elasticsoftware.elasticactors.serialization.internal.ScheduledMessageDeserializer;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutorImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Joost van de Wijgerd
 */
public class BackplaneConfiguration {
    @Autowired
    private Environment env;
    @Autowired
    private InternalActorSystems cluster;
    @Autowired
    private ActorRefFactory actorRefFactory;

    private CqlSession cassandraSession;

    @PostConstruct
    public void initialize() {
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

    @PreDestroy
    public void destroy() {
        this.cassandraSession.close();
    }

    @Bean(name = {"asyncUpdateExecutor"}, destroyMethod = "shutdown")
    public ThreadBoundExecutor createAsyncUpdateExecutor() {
        final int workers = env.getProperty("ea.asyncUpdateExecutor.workerCount",Integer.class,Runtime.getRuntime().availableProcessors() * 3);
        final int batchSize = env.getProperty("ea.asyncUpdateExecutor.batchSize",Integer.class,20);
        final boolean optimizedV1Batches = env.getProperty("ea.asyncUpdateExecutor.optimizedV1Batches", Boolean.TYPE, true);
        return new ThreadBoundExecutorImpl(new PersistentActorUpdateEventProcessor(cassandraSession, batchSize, optimizedV1Batches),batchSize,new DaemonThreadFactory("UPDATE-EXECUTOR-WORKER"),workers);
    }

    @Bean(name = {"persistentActorRepository"})
    public PersistentActorRepository getPersistentActorRepository(@Qualifier("asyncUpdateExecutor") ThreadBoundExecutor asyncUpdateExecutor) {
        final Integer compressionThreshold = env.getProperty("ea.persistentActorRepository.compressionThreshold",Integer.class, 512);
        Serializer serializer = new CompressingSerializer<>(new PersistentActorSerializer(cluster),compressionThreshold);
        Deserializer deserializer = new DecompressingDeserializer<>(new PersistentActorDeserializer(actorRefFactory,cluster));
        return new CassandraPersistentActorRepository(cassandraSession, cluster.getClusterName(), asyncUpdateExecutor, serializer, deserializer);
    }

    @Bean(name = {"scheduledMessageRepository"})
    public ScheduledMessageRepository getScheduledMessageRepository() {
        return new CassandraScheduledMessageRepository(cluster.getClusterName(), cassandraSession, new ScheduledMessageDeserializer(new ActorRefDeserializer(actorRefFactory)));
    }

    @Bean(name = {"actorSystemEventListenerRepository"})
    public ActorSystemEventListenerRepository getActorSystemEventListenerRepository() {
        return new CassandraActorSystemEventListenerRepository(cluster.getClusterName(), cassandraSession);
    }

    @Bean(name = "cassandraHealthCheck")
    public CassandraHealthCheck getHealthCheck() {
        return new CassandraHealthCheck(cassandraSession);
    }
}
