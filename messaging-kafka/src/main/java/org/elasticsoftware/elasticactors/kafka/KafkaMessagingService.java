package org.elasticsoftware.elasticactors.kafka;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.elasticsoftware.elasticactors.PhysicalNode;
import org.elasticsoftware.elasticactors.messaging.MessageHandler;
import org.elasticsoftware.elasticactors.messaging.MessageQueue;
import org.elasticsoftware.elasticactors.messaging.MessageQueueFactory;
import org.elasticsoftware.elasticactors.messaging.MessagingService;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;
import org.elasticsoftware.elasticactors.util.concurrent.ThreadBoundExecutor;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.springframework.util.StringUtils.commaDelimitedListToSet;

/**
 * @author Joost van de Wijgerd
 */
public final class KafkaMessagingService implements MessagingService {
    private final String kafkaHosts;
    private final Integer kafkaPort;
    private final String elasticActorsCluster;
    private final ThreadBoundExecutor queueExecutor;
    private final InternalMessageDeserializer internalMessageDeserializer;

    public KafkaMessagingService(String kafkaHosts,
                                 Integer kafkaPort, String elasticActorsCluster,
                                 ThreadBoundExecutor queueExecutor,
                                 InternalMessageDeserializer internalMessageDeserializer) {
        this.kafkaHosts = kafkaHosts;
        this.kafkaPort = kafkaPort;
        this.elasticActorsCluster = elasticActorsCluster;
        this.queueExecutor = queueExecutor;
        this.internalMessageDeserializer = internalMessageDeserializer;
    }

    @PostConstruct
    public void start() throws IOException {
        // find the Kafka metadata for this cluster
        TopicMetadata topicMetadata = getTopicMetaData();
        if(topicMetadata != null) {
            // now map the partitions to Kafka brokers

        } else {
            // fatal error, we cannot connect to the Kafka cluster at all
            throw new IOException("Unable to connect to Kafka cluster");
        }
    }

    @Override
    public void sendWireMessage(String queueName, byte[] serializedMessage, PhysicalNode receiver) throws IOException {
        // do nothing
    }

    private TopicMetadata getTopicMetaData() {
        Set<String> brokers = commaDelimitedListToSet(this.kafkaHosts);

        // loop through all the brokers until we find an active one
        for (String broker : brokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(broker, this.kafkaPort, 100000, 64 * 1024, "leaderLookup");
                kafka.javaapi.TopicMetadataResponse response = consumer.send(new TopicMetadataRequest(singletonList(this.elasticActorsCluster)));
                if(response.topicsMetadata() != null && !response.topicsMetadata().isEmpty()) {
                    // there should be only one
                    return response.topicsMetadata().get(0);
                }
            } catch(Exception e) {
                // some error occurred communicating to broker, try another
            } finally {
                if(consumer != null) {
                    try {
                        consumer.close();
                    } catch(Exception e) {
                        //ignore
                    }
                }
            }
        }
        // nothing found, handle at the higher level
        return null;
    }

    private final class LocalMessageQueueFactory implements MessageQueueFactory {
        @Override
        public MessageQueue create(String name, MessageHandler messageHandler) throws Exception {
            // do some reverse engineering here, see if it's a shard or a node
            String[] components = name.split("/");
            if("shards".equals(components[1])) {

            }
            // need to find a matching broker (the one that's the leader for this partition)

            return null;
        }
    }


}
