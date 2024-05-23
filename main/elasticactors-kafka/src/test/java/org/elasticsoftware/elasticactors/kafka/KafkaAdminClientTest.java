/*
 * Copyright 2013 - 2024 The Original Authors
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
