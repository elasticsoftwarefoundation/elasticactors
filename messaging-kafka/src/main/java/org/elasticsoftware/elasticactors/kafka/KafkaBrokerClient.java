/*
 * Copyright 2013 - 2016 The Original Authors
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

package org.elasticsoftware.elasticactors.kafka;

import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Joost van de Wijgerd
 */
public final class KafkaBrokerClient implements Runnable {
    private final Integer id;
    private final String kafkaHost;
    private final Integer port;
    private final Set<ManagedPartition> managedPartitions = new HashSet<>();
    private final String clientId;
    private final String kafkaTopic;
    private SimpleConsumer kafkaConsumer;


    public KafkaBrokerClient(String eaHost, String kafkaTopic, Integer id, String kafkaHost, Integer port) {
        this.id = id;
        this.kafkaHost = kafkaHost;
        this.port = port;
        this.clientId = String.format("%s_%d",eaHost,id);
        this.kafkaTopic = kafkaTopic;
    }

    public void initialize() {
        this.kafkaConsumer = new SimpleConsumer(kafkaHost, port, 100000, 64 * 1024, clientId);
    }

    public void addManagedPartition(ManagedPartition partition) {
        this.managedPartitions.add(partition);
    }

    @Override
    public void run() {
        // consumer loop for this broker instance
        while(true) {
            // fetch data for all managed partitions
            FetchRequestBuilder reqBuilder = new FetchRequestBuilder().clientId(clientId);
            for (ManagedPartition managedPartition : managedPartitions) {
                reqBuilder.addFetch(this.kafkaTopic, managedPartition.partitionId, managedPartition.getLastOffset(), 100000);
            }
            // get the next response
            FetchResponse fetchResponse = kafkaConsumer.fetch(reqBuilder.build());
            // check for errors
            if(fetchResponse.hasError()) {
                // see where the error is
                for (ManagedPartition managedPartition : managedPartitions) {
                    short errorCode = fetchResponse.errorCode(this.kafkaTopic,managedPartition.partitionId);
                    if(errorCode != ErrorMapping.NoError()) {
                        // @todo: figure out what the error is, and act accordingly
                    }
                }
            } else {
                // start processing
                for (ManagedPartition managedPartition : managedPartitions) {
                    for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(this.kafkaTopic, managedPartition.partitionId)) {
                        // need to deliver this message to the proper queue

                    }
                }
            }
        }
    }

    public static final class ManagedPartition {
        private final Integer partitionId;
        private Long lastOffset;

        public ManagedPartition(Integer partitionId, Long lastOffset) {
            this.partitionId = partitionId;
            this.lastOffset = lastOffset;
        }

        public Long getLastOffset() {
            return lastOffset;
        }

        public void setLastOffset(Long lastOffset) {
            this.lastOffset = lastOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ManagedPartition that = (ManagedPartition) o;

            if (!partitionId.equals(that.partitionId)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return partitionId.hashCode();
        }
    }
}
