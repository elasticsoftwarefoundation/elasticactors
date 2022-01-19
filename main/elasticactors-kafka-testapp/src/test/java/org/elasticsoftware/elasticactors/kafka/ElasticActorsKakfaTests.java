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

package org.elasticsoftware.elasticactors.kafka;

import org.elasticsoftware.elasticactors.kafka.testapp.KafkaTestApplication;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class ElasticActorsKakfaTests {
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1"));

    @BeforeClass
    public static void setup() {
        kafka.start();
    }

    @AfterClass
    public static void cleanup() {
        kafka.stop();
    }

    @Test
    public void testApp() {
        System.setProperty("ea.kafka.bootstrapServers", kafka.getBootstrapServers());
        KafkaTestApplication.main(new String[0]);
    }
}
