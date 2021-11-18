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

import org.elasticsoftware.elasticactors.cassandra.common.serialization.CompressingSerializer;
import org.elasticsoftware.elasticactors.cassandra.common.serialization.DecompressingDeserializer;
import org.elasticsoftware.elasticactors.cassandra2.cluster.CassandraActorSystemEventListenerRepository;
import org.elasticsoftware.elasticactors.cassandra2.cluster.scheduler.CassandraScheduledMessageRepository;
import org.elasticsoftware.elasticactors.cassandra2.health.CassandraHealthCheck;
import org.elasticsoftware.elasticactors.cassandra2.state.CassandraPersistentActorRepository;
import org.elasticsoftware.elasticactors.cassandra2.state.PersistentActorUpdateEventProcessor;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * @author Joost van de Wijgerd
 */
public class BackplaneConfiguration {

    @Bean(name = {"cassandraSessionManager"})
    public CassandraSessionManager createCassandraSessionManager(Environment env) {
        return new CassandraSessionManager(env);
    }

    @Bean(name = {"asyncUpdateExecutor"}, destroyMethod = "shutdown")
    public ThreadBoundExecutor createAsyncUpdateExecutor(
        Environment env,
        CassandraSessionManager cassandraSessionManager)
    {
        final int workers = env.getProperty("ea.asyncUpdateExecutor.workerCount",Integer.class,Runtime.getRuntime().availableProcessors() * 3);
        final int batchSize = env.getProperty("ea.asyncUpdateExecutor.batchSize",Integer.class,20);
        final boolean optimizedV1Batches = env.getProperty("ea.asyncUpdateExecutor.optimizedV1Batches", Boolean.TYPE, true);
        return new ThreadBoundExecutorImpl(
            new PersistentActorUpdateEventProcessor(
                cassandraSessionManager.getSession(),
                batchSize,
                optimizedV1Batches
            ),
            batchSize,
            new DaemonThreadFactory("UPDATE-EXECUTOR-WORKER"),
            workers
        );
    }

    @Bean(name = {"persistentActorRepository"})
    public PersistentActorRepository getPersistentActorRepository(
        @Qualifier("asyncUpdateExecutor") ThreadBoundExecutor asyncUpdateExecutor,
        InternalActorSystems cluster,
        ActorRefFactory actorRefFactory,
        CassandraSessionManager cassandraSessionManager,
        Environment env)
    {
        final Integer compressionThreshold = env.getProperty("ea.persistentActorRepository.compressionThreshold",Integer.class, 512);
        Serializer serializer = new CompressingSerializer<>(new PersistentActorSerializer(cluster),compressionThreshold);
        Deserializer deserializer = new DecompressingDeserializer<>(new PersistentActorDeserializer(actorRefFactory,cluster));
        return new CassandraPersistentActorRepository(
            cassandraSessionManager.getSession(),
            cluster.getClusterName(),
            asyncUpdateExecutor,
            serializer,
            deserializer
        );
    }

    @Bean(name = {"scheduledMessageRepository"})
    public ScheduledMessageRepository getScheduledMessageRepository(
        InternalActorSystems cluster,
        ActorRefFactory actorRefFactory,
        CassandraSessionManager cassandraSessionManager)
    {
        return new CassandraScheduledMessageRepository(
            cluster.getClusterName(),
            cassandraSessionManager.getSession(),
            new ScheduledMessageDeserializer(new ActorRefDeserializer(actorRefFactory))
        );
    }

    @Bean(name = {"actorSystemEventListenerRepository"})
    public ActorSystemEventListenerRepository getActorSystemEventListenerRepository(
        InternalActorSystems cluster,
        CassandraSessionManager cassandraSessionManager)
    {
        return new CassandraActorSystemEventListenerRepository(
            cluster.getClusterName(),
            cassandraSessionManager.getSession()
        );
    }

    @Bean(name = "cassandraHealthCheck")
    public CassandraHealthCheck getHealthCheck(CassandraSessionManager cassandraSessionManager) {
        return new CassandraHealthCheck(cassandraSessionManager.getSession());
    }
}
