package org.elasticsoftware.elasticactors.kafka.utils;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.admin.*;
import org.elasticsoftware.elasticactors.NodeKey;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.TopicConfig.*;

public final class TopicHelper {
    private static final Short DEFAULT_REPLICATION_FACTOR = new Short("3");

    private TopicHelper() {

    }

    public static void ensureTopicsExists(String bootstrapServers, String nodeId, int nodePartitions, InternalActorSystem internalActorSystem) throws Exception {
        //"bootstrap.servers"
        Map<String, Object> adminClientConfig = new HashMap<>();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AdminClient adminClient = AdminClient.create(adminClientConfig);

        // ensure all the topics are created
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Map<String, TopicListing> availableTopics = listTopicsResult.namesToListings().get();

        // shard message queues
        String messagesTopicName = TopicNamesHelper.getMessagesTopic(internalActorSystem);
        if(!availableTopics.containsKey(messagesTopicName)) {
            // need to create a new topic
            NewTopic topic = new NewTopic(messagesTopicName, internalActorSystem.getNumberOfShards(), DEFAULT_REPLICATION_FACTOR);
            Map<String, String> configs = new HashMap<>();
            configs.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE);
            configs.put(MAX_MESSAGE_BYTES_CONFIG, "10485760");  // 1MB max message size
            configs.put(MIN_IN_SYNC_REPLICAS_CONFIG, "2"); // QUORUM writes
            configs.put(RETENTION_MS_CONFIG, "604800000"); // 7 day log retention for the messages
            configs.put(SEGMENT_MS_CONFIG, "604800000"); // roll over after 7 days even if not full
            topic.configs(configs);
            CreateTopicsResult result = adminClient.createTopics(Lists.newArrayList(topic));
            result.all().get(); // @todo: see if we didn't get an exception
        }

        // (local) node message queue
        String nodeMessagesTopicName = TopicNamesHelper.getNodeMessagesTopic(internalActorSystem, nodeId);
        if(!availableTopics.containsKey(nodeMessagesTopicName)) {
            NewTopic topic = new NewTopic(nodeMessagesTopicName, nodePartitions, DEFAULT_REPLICATION_FACTOR);
            Map<String, String> configs = new HashMap<>();
            configs.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE);
            configs.put(MAX_MESSAGE_BYTES_CONFIG, "10485760");  // 1MB max message size
            configs.put(MIN_IN_SYNC_REPLICAS_CONFIG, "2"); // QUORUM writes
            configs.put(RETENTION_MS_CONFIG, "604800000"); // 7 day log retention for the messages
            configs.put(SEGMENT_MS_CONFIG, "604800000"); // roll over after 7 days even if not full
            topic.configs(configs);
            CreateTopicsResult result = adminClient.createTopics(Lists.newArrayList(topic));
            result.all().get(); // @todo: see if we didn't get an exception
        }

        // persistent actorstate compacted queue
        String persistentActorsTopicName =  TopicNamesHelper.getPersistentActorsTopic(internalActorSystem);
        if(!availableTopics.containsKey(persistentActorsTopicName)) {
            NewTopic topic = new NewTopic(persistentActorsTopicName, internalActorSystem.getNumberOfShards(), DEFAULT_REPLICATION_FACTOR);
            Map<String, String> configs = new HashMap<>();
            configs.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
            configs.put(DELETE_RETENTION_MS_CONFIG, "86400000");  // 1 day tombstone retention
            configs.put(MAX_MESSAGE_BYTES_CONFIG, "20971520");  // 2MB max message size
            configs.put(MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.5"); // waste no more than 50%
            configs.put(MIN_IN_SYNC_REPLICAS_CONFIG, "2"); // QUORUM writes
            configs.put(RETENTION_MS_CONFIG, ""+Long.MAX_VALUE); // forever log retention for the messages
            configs.put(SEGMENT_MS_CONFIG, "604800000"); // roll over after 7 days even if not full
            topic.configs(configs);
            CreateTopicsResult result = adminClient.createTopics(Lists.newArrayList(topic));
            result.all().get(); // @todo: see if we didn't get an exception
        }

        // schedules messages compacted queue
        String scheduledMessagesTopicName = TopicNamesHelper.getScheduledMessagesTopic(internalActorSystem);
        if(!availableTopics.containsKey(scheduledMessagesTopicName)) {
            NewTopic topic = new NewTopic(scheduledMessagesTopicName, internalActorSystem.getNumberOfShards(), DEFAULT_REPLICATION_FACTOR);
            Map<String, String> configs = new HashMap<>();
            configs.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
            configs.put(DELETE_RETENTION_MS_CONFIG, "86400000");  // 1 day tombstone retention
            configs.put(MAX_MESSAGE_BYTES_CONFIG, "20971520");  // 2MB max message size
            configs.put(MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.5"); // waste no more than 50%
            configs.put(MIN_IN_SYNC_REPLICAS_CONFIG, "2"); // QUORUM writes
            configs.put(RETENTION_MS_CONFIG, ""+Long.MAX_VALUE); // forever log retention for the messages
            configs.put(SEGMENT_MS_CONFIG, "604800000"); // roll over after 7 days even if not full
            topic.configs(configs);
            CreateTopicsResult result = adminClient.createTopics(Lists.newArrayList(topic));
            result.all().get(); // @todo: see if we didn't get an exception
        }

        String actorSystemEventListenersTopicName = TopicNamesHelper.getActorsystemEventListenersTopic(internalActorSystem);
        if(!availableTopics.containsKey(actorSystemEventListenersTopicName)) {
            NewTopic topic = new NewTopic(actorSystemEventListenersTopicName, internalActorSystem.getNumberOfShards(), DEFAULT_REPLICATION_FACTOR);
            Map<String, String> configs = new HashMap<>();
            configs.put(CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_COMPACT);
            configs.put(DELETE_RETENTION_MS_CONFIG, "86400000");  // 1 day tombstone retention
            configs.put(MAX_MESSAGE_BYTES_CONFIG, "20971520");  // 2MB max message size
            configs.put(MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.5"); // waste no more than 50%
            configs.put(MIN_IN_SYNC_REPLICAS_CONFIG, "2"); // QUORUM writes
            configs.put(RETENTION_MS_CONFIG, ""+Long.MAX_VALUE); // forever log retention for the messages
            configs.put(SEGMENT_MS_CONFIG, "604800000"); // roll over after 7 days even if not full
            topic.configs(configs);
            CreateTopicsResult result = adminClient.createTopics(Lists.newArrayList(topic));
            result.all().get(); // @todo: see if we didn't get an exception
        }
    }
}
