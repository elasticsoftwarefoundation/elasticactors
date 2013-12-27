package org.elasticsoftware.elasticactors.configuration;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import org.elasticsoftware.elasticactors.cassandra.state.CassandraPersistentActorRepository;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystems;
import org.elasticsoftware.elasticactors.serialization.internal.PersistentActorDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.PersistentActorSerializer;
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
    private ColumnFamilyTemplate<String,String> persistentActorsColumnFamilyTemplate;

    @PostConstruct
    public void initialize() {
        String cassandraHosts = env.getProperty("ea.cassandra.hosts","localhost:9160");
        CassandraHostConfigurator hostConfigurator = new CassandraHostConfigurator(cassandraHosts);
        hostConfigurator.setAutoDiscoverHosts(false);
        hostConfigurator.setMaxActive(16);
        hostConfigurator.setRetryDownedHosts(false);
        hostConfigurator.setMaxWaitTimeWhenExhausted(1000L);
        String cassandraClusterName = env.getProperty("ea.cassandra.cluster","ElasticActorsCluster");
        Cluster cluster = HFactory.getOrCreateCluster(cassandraClusterName,hostConfigurator);
        String cassandraKeyspaceName = env.getProperty("ea.cassandra.keyspace","ElasticActors");
        Keyspace keyspace = HFactory.createKeyspace(cassandraKeyspaceName,cluster);
        persistentActorsColumnFamilyTemplate =
            new ThriftColumnFamilyTemplate<>(keyspace,"PeristentActors", StringSerializer.get(),StringSerializer.get());
    }

    @Bean(name = {"persistentActorRepository"})
    public PersistentActorRepository getPersistentActorRepository() {
        CassandraPersistentActorRepository persistentActorRepository = new CassandraPersistentActorRepository();
        persistentActorRepository.setColumnFamilyTemplate(persistentActorsColumnFamilyTemplate);
        persistentActorRepository.setSerializer(new PersistentActorSerializer(cluster));
        persistentActorRepository.setDeserializer(new PersistentActorDeserializer(actorRefFactory,cluster));
        return persistentActorRepository;
    }
}

