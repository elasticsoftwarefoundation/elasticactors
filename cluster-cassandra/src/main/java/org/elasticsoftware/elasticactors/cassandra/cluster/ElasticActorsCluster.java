/*
 * Copyright 2013 the original authors
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

package org.elasticsoftware.elasticactors.cassandra.cluster;

import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.model.QuorumAllConsistencyLevelPolicy;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import org.apache.log4j.Logger;
import org.elasticsoftware.elasticactors.ActorRef;
import org.elasticsoftware.elasticactors.ActorSystemConfiguration;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.*;
import org.elasticsoftware.elasticactors.dependencies.Graph;
import org.elasticsoftware.elasticactors.dependencies.NodeValueListener;
import org.elasticsoftware.elasticactors.serialization.MessageDeserializer;
import org.elasticsoftware.elasticactors.serialization.MessageSerializer;
import org.elasticsoftware.elasticactors.serialization.internal.SystemDeserializers;
import org.elasticsoftware.elasticactors.serialization.internal.SystemSerializers;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundRunnable;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Joost van de Wijgerd
 */
@Configurable
public final class ElasticActorsCluster implements ActorRefFactory, ApplicationContextAware, ClusterEventListener, InternalActorSystems, ConsistencyLevelPolicy {
    private static final Logger logger = Logger.getLogger(ElasticActorsCluster.class);
    private static final AtomicReference<ElasticActorsCluster> INSTANCE = new AtomicReference<ElasticActorsCluster>(null);
    private final ConcurrentMap<String, LocalActorSystemInstance> managedActorSystems = new ConcurrentHashMap<String, LocalActorSystemInstance>();
    private final AtomicBoolean clusterStarted = new AtomicBoolean(false);
    private String clusterName;
    private PhysicalNode localNode;
    private NodeSelectorFactory nodeSelectorFactory;
    private ThreadBoundExecutor<String> executor;
    private final SystemSerializers systemSerializers = new SystemSerializers(this);
    private final SystemDeserializers systemDeserializers = new SystemDeserializers(this);
    private ActorSystemRepository actorSystemRepository;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("CLUSTER_SCHEDULER"));
    private Cluster cassandraCluster;
    private static final ConsistencyLevelPolicy DEFAULT_CONSISTENCY_LEVEL_POLICY = new QuorumAllConsistencyLevelPolicy();
    private static final ConsistencyLevelPolicy FALLBACK_CONSISTENCY_LEVEL_POLICY = new AllOneConsistencyLevelPolicy();
    private final AtomicReference<ConsistencyLevelPolicy> consistencyLevelPolicy = new AtomicReference<ConsistencyLevelPolicy>(FALLBACK_CONSISTENCY_LEVEL_POLICY);


    public static ElasticActorsCluster getInstance() {
        return INSTANCE.get();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        INSTANCE.set(applicationContext.getBean(ElasticActorsCluster.class));
    }

    @Override
    public void onJoined(String hostId, InetAddress hostAddress) throws Exception {
        this.localNode = new PhysicalNodeImpl(hostId, hostAddress, true);
        logger.info(String.format("%s running, starting ElasticActors Runtime", localNode.toString()));
        // start NodeSelectorFactory
        nodeSelectorFactory.start();


    }

    private void loadActorSystems() {
        // load all actor systems (but not start them yet since we are not officially part of the cluster)
        List<RegisteredActorSystem> registeredActorSystems = actorSystemRepository.findAll();
        logger.info(String.format("Loading %d ActorSystems", registeredActorSystems.size()));
        for (RegisteredActorSystem registeredActorSystem : registeredActorSystems) {
            logger.info(String.format("Loading ActorSystem [%s] with configuration class [%s]",
                    registeredActorSystem.getName(), registeredActorSystem.getConfigurationClass()));
            try {
                Class<? extends ActorSystemConfiguration> configurationClass =
                        (Class<? extends ActorSystemConfiguration>) Class.forName(registeredActorSystem.getConfigurationClass());
                Constructor<? extends ActorSystemConfiguration> constructor =
                        ClassUtils.getConstructorIfAvailable(configurationClass, String.class, Integer.TYPE);
                if (constructor != null) {
                    ActorSystemConfiguration configuration = constructor.newInstance(registeredActorSystem.getName(),
                            registeredActorSystem.getNrOfShards());
                    managedActorSystems.put(registeredActorSystem.getName(),
                            new LocalActorSystemInstance(this.localNode, this, configuration, nodeSelectorFactory));
                } else {
                    logger.warn(String.format("No matching constructor(String,int) found on configuration class [%s]",
                            registeredActorSystem.getConfigurationClass()));
                    ActorSystemConfiguration configuration = configurationClass.newInstance();
                    managedActorSystems.put(configuration.getName(),
                            new LocalActorSystemInstance(this.localNode, this, configuration, nodeSelectorFactory));
                }
                logger.info(String.format("Loaded ActorSystem [%s] with configuration class [%s]",
                        registeredActorSystem.getName(), registeredActorSystem.getConfigurationClass()));
            } catch (Throwable e) {
                logger.error(String.format("Exception while initializing ActorSystem [%s] with configuration class [%s]",
                        registeredActorSystem.getName(),
                        registeredActorSystem.getConfigurationClass()), e);
            }
        }
    }

    @Override
    public void onTopologyChanged(final Map<InetAddress, String> topology) {
        logger.info("Cluster topology changed");
        scheduledExecutorService.execute(new Runnable() {

            @Override
            public void run() {
                final List<PhysicalNode> clusterNodes = new LinkedList<PhysicalNode>();
                for (Map.Entry<InetAddress, String> hostEntry : topology.entrySet()) {
                    if (localNode != null && localNode.getId().equals(hostEntry.getValue())) {
                        clusterNodes.add(localNode);
                    } else {
                        clusterNodes.add(new PhysicalNodeImpl(hostEntry.getValue(), hostEntry.getKey(), false));
                    }
                }
                // set correct consistency level
                if (topology.size() >= 2) {
                    consistencyLevelPolicy.set(DEFAULT_CONSISTENCY_LEVEL_POLICY);
                } else {
                    consistencyLevelPolicy.set(FALLBACK_CONSISTENCY_LEVEL_POLICY);
                }

                logger.info("New Cluster view: " + clusterNodes.toString());
                // see if it's the first time
                if (localNode != null) {
                    if (clusterStarted.compareAndSet(false, true)) {
                        logger.info("Initial startup detected, scheduling ActorSystem loading sequence");
                        // we need a delay here because thrift will start listening after this event
                        scheduledExecutorService.schedule(new Runnable() {
                            @Override
                            public void run() {
                                logger.info("Loading ActorSystems...");
                                try {
                                    // some trickery to get hector to work
                                    cassandraCluster.addHost(new CassandraHost(String.format("%s:9160", localNode.getAddress().getHostAddress())), false);
                                    //ensureKeyspace();
                                    loadActorSystems();
                                    rebalance(clusterNodes);
                                } catch (Exception e) {
                                    logger.error("Exception while loading ActorSystems", e);
                                }
                            }
                        }, 1000, TimeUnit.MILLISECONDS);

                    } else {
                        rebalance(clusterNodes);
                    }
                }
            }
        });

    }

    private void ensureKeyspace() {
        List<KeyspaceDefinition> keyspaces = cassandraCluster.describeKeyspaces();
        boolean existing = false;
        for (KeyspaceDefinition keyspace : keyspaces) {
            if (keyspace.getName().equals("ElasticActors")) {
                existing = true;
                break;
            }
        }
        if (!existing) {
            logger.info("ElasticActors Keyspace not found, creating");
            List<ColumnFamilyDefinition> cfDefs = new LinkedList<ColumnFamilyDefinition>();
            ColumnFamilyDefinition actorSystems = new ThriftCfDef("ElasticActors", "ActorSystems");
            KeyspaceDefinition ksDef = new ThriftKsDef("ElasticActors", ThriftKsDef.NETWORK_TOPOLOGY_STRATEGY, 1, cfDefs);
            cassandraCluster.addKeyspace(ksDef, true);
        }
    }

    private void rebalance(List<PhysicalNode> clusterNodes) {
        // need to resolve the dependencies here
        final List<LocalActorSystemInstance> nodeValueList = new ArrayList<LocalActorSystemInstance>();
        Graph<LocalActorSystemInstance> dependencyGraph = new Graph<LocalActorSystemInstance>(new NodeValueListener<LocalActorSystemInstance>() {
            public void evaluating(LocalActorSystemInstance nodeValue) {
                nodeValueList.add(nodeValue);
            }
        });

        for (LocalActorSystemInstance actorSystemInstance : managedActorSystems.values()) {
            List<String> dependencies = actorSystemInstance.getDependencies();

            for (String dependency : dependencies) {
                LocalActorSystemInstance instance = managedActorSystems.get(dependency);
                if (instance != null) {
                    logger.info(String.format("Adding dependency from [%s] on [%s]", actorSystemInstance.getName(), instance.getName()));
                    dependencyGraph.addDependency(instance, actorSystemInstance);
                } else {
                    // just add the node to the back of the list, order doesn't matter
                    // @todo: this is not correct
                    // nodeValueList.add(actorSystemInstance);
                }
            }
        }

        dependencyGraph.generateDependencies();
        // see if we have all of the original members
        for (LocalActorSystemInstance actorSystemInstance : managedActorSystems.values()) {
            if (!nodeValueList.contains(actorSystemInstance)) {
                nodeValueList.add(actorSystemInstance);
            }
        }
        logger.info(String.format("Processing ActorSystems in the following order: [%s]", nodeValueList.toString()));

        for (LocalActorSystemInstance actorSystemInstance : nodeValueList) {
            //new RebalancingRunnable(actorSystemInstance, clusterNodes).run();
            scheduledExecutorService.execute(new RebalancingRunnable(actorSystemInstance,clusterNodes));
        }
    }

    private final class RebalancingRunnable implements ThreadBoundRunnable<String> {
        private final LocalActorSystemInstance actorSystemInstance;
        private final List<PhysicalNode> clusterNodes;

        private RebalancingRunnable(LocalActorSystemInstance actorSystemInstance, List<PhysicalNode> clusterNodes) {
            this.actorSystemInstance = actorSystemInstance;
            this.clusterNodes = clusterNodes;
        }

        @Override
        public String getKey() {
            return actorSystemInstance.getName();
        }

        @Override
        public void run() {
            logger.info(String.format("Updating %d nodes for ActorSystem[%s]", clusterNodes.size(), actorSystemInstance.getName()));
            try {
                actorSystemInstance.updateNodes(clusterNodes);
            } catch (Exception e) {
                logger.error(String.format("ActorSystem[%s] failed to update nodes", actorSystemInstance.getName()), e);
            }
            logger.info(String.format("Rebalancing %d shards for ActorSystem[%s]", actorSystemInstance.getNumberOfShards(), actorSystemInstance.getName()));
            try {
                actorSystemInstance.distributeShards(clusterNodes);
            } catch (Exception e) {
                logger.error(String.format("ActorSystem[%s] failed to (re-)distribute shards", actorSystemInstance.getName()), e);
            }
        }
    }

    @Override
    public void onLeft() {
        // shutdown all actor systems
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public ActorRefFactory getActorRefFactory() {
        return this;
    }

    @Override
    public InternalActorSystem get(String actorSystemName) {
        return managedActorSystems.get(actorSystemName);
    }

    @Override
    public <T> MessageSerializer<T> getSystemMessageSerializer(Class<T> messageClass) {
        return systemSerializers.get(messageClass);
    }

    @Override
    public <T> MessageDeserializer<T> getSystemMessageDeserializer(Class<T> messageClass) {
        return systemDeserializers.get(messageClass);
    }

    @Value("${elasticactors.cluster.name}")
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    public ActorRef create(String refSpec) throws IllegalArgumentException {
        return ActorRefTools.parse(refSpec, this);
    }

    @Override
    public HConsistencyLevel get(OperationType op) {
        return consistencyLevelPolicy.get().get(op);
    }

    @Override
    public HConsistencyLevel get(OperationType op, String cfName) {
        return consistencyLevelPolicy.get().get(op, cfName);
    }

    @Autowired
    public void setNodeSelectorFactory(NodeSelectorFactory nodeSelectorFactory) {
        this.nodeSelectorFactory = nodeSelectorFactory;
    }

    @Autowired
    public void setExecutor(@Qualifier("clusterExecutor") ThreadBoundExecutor<String> executor) {
        this.executor = executor;
    }

    @Autowired
    public void setActorSystemRepository(ActorSystemRepository actorSystemRepository) {
        this.actorSystemRepository = actorSystemRepository;
    }

    @Autowired
    public void setCassandraCluster(Cluster cassandraCluster) {
        this.cassandraCluster = cassandraCluster;
    }
}
