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

package org.elasticsoftware.elasticactors.kafka.cluster;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.cluster.ClusterEventListener;
import org.elasticsoftware.elasticactors.cluster.ClusterMessageHandler;
import org.elasticsoftware.elasticactors.cluster.ClusterService;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.kafka.utils.TopicHelper;
import org.elasticsoftware.elasticactors.kafka.utils.TopicNamesHelper;
import org.elasticsoftware.elasticactors.util.concurrent.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

import static java.util.Collections.singletonList;
import static org.elasticsoftware.elasticactors.kafka.utils.TopicNamesHelper.getMessagesTopic;

public class KafkaClusterService implements ClusterService, ConsumerRebalanceListener {
    private static final Logger logger = LoggerFactory.getLogger(KafkaClusterService.class);
    private final Queue<ClusterEventListener> eventListeners = new ConcurrentLinkedQueue<>();
    private ClusterMessageHandler clusterMessageHandler;
    private final PhysicalNode localNode;
    private final Consumer<String,byte[]> clusterServiceConsumer;

    public KafkaClusterService(String clusterName,
                               PhysicalNode localNode,
                               String bootstrapServers) {
        this.localNode = localNode;
        final Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfig.put("internal.leave.group.on.close", false);
        consumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase(Locale.ROOT));
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, clusterName+"-ClusterService");
        consumerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, localNode.getId()+"-ClusterService");
        clusterServiceConsumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer());
    }


    @Override
    public void reportReady() {
        Executors.newSingleThreadExecutor(new DaemonThreadFactory("KafkaClusterService")).submit(() -> {
            clusterServiceConsumer.subscribe(singletonList("ElasticActors_Messages-kafkatest.local-kafkatest"), this);
            boolean running = true;
            while (running) {
                try {
                    // this will cause the balancing check to happen
                    clusterServiceConsumer.poll(Duration.ofMillis(10));
                } catch(WakeupException | InterruptException e) {
                    logger.warn("Recoverable exception while polling for Messages", e);
                } catch(KafkaException e) {
                    logger.error("FATAL: Unrecoverable exception while polling for Messages", e);
                    running = false;
                } catch(Throwable t) {
                    logger.error("Unexpected exception while polling for Messages", t);
                    running = false;
                }
            }
        });
    }

    @Override
    public void reportPlannedShutdown() {

    }

    @Override
    public void addEventListener(ClusterEventListener eventListener) {
        eventListeners.add(eventListener);
    }

    @Override
    public void removeEventListener(ClusterEventListener eventListener) {
        this.eventListeners.remove(eventListener);
    }

    @Override
    public void sendMessage(String memberToken, byte[] message) throws Exception {
        //@todo: send to local?
    }

    @Override
    public void setClusterMessageHandler(ClusterMessageHandler clusterMessageHandler) {
        this.clusterMessageHandler = clusterMessageHandler;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("Got onPartitionsRevoked");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("Got onPartitionsAssigned");
        for (ClusterEventListener eventListener : eventListeners) {
            try {
                eventListener.onTopologyChanged(singletonList(localNode));
            } catch (Exception e) {
                logger.error("Exception in onMasterElected",e);
            }
            try {
                eventListener.onMasterElected(localNode);
            } catch (Exception e) {
                logger.error("Exception in onMasterElected",e);
            }
        }
    }

}
