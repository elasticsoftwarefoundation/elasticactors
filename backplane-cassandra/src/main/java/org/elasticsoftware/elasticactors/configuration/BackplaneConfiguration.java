/*
 * Copyright 2013 - 2014 The Original Authors
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

package org.elasticsoftware.elasticactors.configuration;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.factory.HFactory;
import org.elasticsoftware.elasticactors.cassandra.cluster.scheduler.CassandraScheduledMessageRepository;
import org.elasticsoftware.elasticactors.cassandra.serialization.CompressingSerializer;
import org.elasticsoftware.elasticactors.cassandra.serialization.DecompressingDeserializer;
import org.elasticsoftware.elasticactors.cassandra.state.CassandraPersistentActorRepository;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.cluster.scheduler.ScheduledMessageRepository;
import org.elasticsoftware.elasticactors.serialization.internal.ActorRefDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.PersistentActorDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.PersistentActorSerializer;
import org.elasticsoftware.elasticactors.serialization.internal.ScheduledMessageDeserializer;
import org.elasticsoftware.elasticactors.state.PersistentActorRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;

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
    private ColumnFamilyTemplate<Composite,String> persistentActorsColumnFamilyTemplate;
    private ColumnFamilyTemplate<Composite,Composite> scheduledMessagesColumnFamilyTemplate;

    @PostConstruct
    public void initialize() {
        String cassandraHosts = env.getProperty("ea.cassandra.hosts","localhost:9160");
        CassandraHostConfigurator hostConfigurator = new CassandraHostConfigurator(cassandraHosts);
        hostConfigurator.setAutoDiscoverHosts(false);
        hostConfigurator.setMaxActive(env.getProperty("ea.cassandra.maxActive",Integer.class,Runtime.getRuntime().availableProcessors() * 3));
        hostConfigurator.setRetryDownedHosts(true);
        hostConfigurator.setMaxWaitTimeWhenExhausted(1000L);
        String cassandraClusterName = env.getProperty("ea.cassandra.cluster","ElasticActorsCluster");
        Cluster cluster = HFactory.getOrCreateCluster(cassandraClusterName,hostConfigurator);
        String cassandraKeyspaceName = env.getProperty("ea.cassandra.keyspace","ElasticActors");
        Keyspace keyspace = HFactory.createKeyspace(cassandraKeyspaceName,cluster);
        persistentActorsColumnFamilyTemplate =
            new ThriftColumnFamilyTemplate<>(keyspace,"PersistentActors", CompositeSerializer.get(),StringSerializer.get());
        scheduledMessagesColumnFamilyTemplate =
            new ThriftColumnFamilyTemplate<>(keyspace,"ScheduledMessages",CompositeSerializer.get(), CompositeSerializer.get());
    }

    @Bean(name = {"persistentActorRepository"})
    public PersistentActorRepository getPersistentActorRepository() {
        CassandraPersistentActorRepository persistentActorRepository = new CassandraPersistentActorRepository(cluster.getClusterName());
        persistentActorRepository.setColumnFamilyTemplate(persistentActorsColumnFamilyTemplate);
        persistentActorRepository.setSerializer(new CompressingSerializer<>(new PersistentActorSerializer(cluster),512));
        persistentActorRepository.setDeserializer(new DecompressingDeserializer<>(new PersistentActorDeserializer(actorRefFactory,cluster)));
        return persistentActorRepository;
    }

    @Bean(name = {"scheduledMessageRepository"})
    public ScheduledMessageRepository getScheduledMessageRepository() {
        CassandraScheduledMessageRepository scheduledMessageRepository = new CassandraScheduledMessageRepository(cluster.getClusterName(), scheduledMessagesColumnFamilyTemplate, new ScheduledMessageDeserializer(new ActorRefDeserializer(actorRefFactory)));
        return scheduledMessageRepository;
    }
}

