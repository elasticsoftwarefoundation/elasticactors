package org.elasticsoftware.elasticactors.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertNotNull;

public class KafkaAdminClientTest {
    @Test(enabled = false)
    public void testListTopics() throws Exception {
        Map<String, Object> adminClientConfig = new HashMap<>();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "api.local.getbux.com:32400,api.local.getbux.com:32401,api.local.getbux.com:32402");
        adminClientConfig.put(AdminClientConfig.CLIENT_ID_CONFIG, "test-adminClient");
        AdminClient adminClient = AdminClient.create(adminClientConfig);

        // ensure all the topics are created
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Map<String, TopicListing> availableTopics = listTopicsResult.namesToListings().get();

        assertNotNull(availableTopics);
    }
}
