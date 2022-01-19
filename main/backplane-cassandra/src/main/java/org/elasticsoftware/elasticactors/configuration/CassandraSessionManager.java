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

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.factory.HFactory;
import org.springframework.core.env.Environment;

public class CassandraSessionManager {

    private final ColumnFamilyTemplate<Composite,String> persistentActorsColumnFamilyTemplate;
    private final ColumnFamilyTemplate<Composite,Composite> scheduledMessagesColumnFamilyTemplate;
    private final ColumnFamilyTemplate<Composite,String> actorSystemEventListenersColumnFamilyTemplate;

    public CassandraSessionManager(Environment env) {
        String cassandraHosts = env.getProperty("ea.cassandra.hosts","localhost:9160");
        CassandraHostConfigurator hostConfigurator = new CassandraHostConfigurator(cassandraHosts);
        hostConfigurator.setAutoDiscoverHosts(false);
        hostConfigurator.setMaxActive(env.getProperty("ea.cassandra.maxActive",Integer.class,Runtime.getRuntime().availableProcessors() * 3));
        hostConfigurator.setRetryDownedHosts(true);
        hostConfigurator.setRetryDownedHostsDelayInSeconds(env.getProperty("ea.cassandra.retryDownedHostsDelayInSeconds",Integer.class,1));
        hostConfigurator.setMaxWaitTimeWhenExhausted(2000L);
        String cassandraClusterName = env.getProperty("ea.cassandra.cluster","ElasticActorsCluster");
        // it seems that there are issues with the CassandraHostRetryService and retrying downed hosts
        // if we don't let the HFactory manage the cluster then CassandraHostRetryService doesn't try to
        // be smart about finding out if a host was removed from the ring and so it will keep on retrying
        // all configured hosts (and ultimately fail-back when the host comes back online)
        // the default is TRUE, which will let HFactory manage the cluster
        Boolean manageClusterThroughHFactory = env.getProperty("ea.cassandra.hfactory.manageCluster", Boolean.class, Boolean.TRUE);
        Cluster cluster;
        if(manageClusterThroughHFactory) {
            cluster = HFactory.getOrCreateCluster(cassandraClusterName, hostConfigurator);
        } else {
            cluster = new ThriftCluster(cassandraClusterName, hostConfigurator, null);
        }
        String cassandraKeyspaceName = env.getProperty("ea.cassandra.keyspace","ElasticActors");
        Keyspace keyspace = HFactory.createKeyspace(cassandraKeyspaceName,cluster);
        persistentActorsColumnFamilyTemplate =
            new ThriftColumnFamilyTemplate<>(keyspace,"PersistentActors", CompositeSerializer.get(),
                StringSerializer.get());
        scheduledMessagesColumnFamilyTemplate =
            new ThriftColumnFamilyTemplate<>(keyspace,"ScheduledMessages",CompositeSerializer.get(), CompositeSerializer.get());
        actorSystemEventListenersColumnFamilyTemplate =
            new ThriftColumnFamilyTemplate<>(keyspace,"ActorSystemEventListeners", CompositeSerializer.get(),StringSerializer.get());
        // return
        // @TODO: make this configurable and use the ColumnSliceIterator
        scheduledMessagesColumnFamilyTemplate.setCount(Integer.MAX_VALUE);
        actorSystemEventListenersColumnFamilyTemplate.setCount(Integer.MAX_VALUE);
    }

    public ColumnFamilyTemplate<Composite, String> getPersistentActorsColumnFamilyTemplate() {
        return persistentActorsColumnFamilyTemplate;
    }

    public ColumnFamilyTemplate<Composite, Composite> getScheduledMessagesColumnFamilyTemplate() {
        return scheduledMessagesColumnFamilyTemplate;
    }

    public ColumnFamilyTemplate<Composite, String> getActorSystemEventListenersColumnFamilyTemplate() {
        return actorSystemEventListenersColumnFamilyTemplate;
    }
}
