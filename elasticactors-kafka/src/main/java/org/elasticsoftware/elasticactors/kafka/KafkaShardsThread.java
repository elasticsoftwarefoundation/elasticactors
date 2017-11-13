package org.elasticsoftware.elasticactors.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsoftware.elasticactors.cluster.ActorRefFactory;
import org.elasticsoftware.elasticactors.cluster.InternalActorSystem;
import org.elasticsoftware.elasticactors.kafka.serialization.KafkaInternalMessageDeserializer;
import org.elasticsoftware.elasticactors.messaging.InternalMessage;
import org.elasticsoftware.elasticactors.serialization.internal.ActorRefDeserializer;
import org.elasticsoftware.elasticactors.serialization.internal.InternalMessageDeserializer;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.requests.IsolationLevel.READ_COMMITTED;

public class KafkaShardsThread extends Thread {
    private static final AtomicInteger THREAD_ID_SEQUENCE = new AtomicInteger(1);
    private final KafkaConsumer<String, InternalMessage> consumer;
    //private final KafkaProducer<String, InternalMessage> producer;
    private final InternalActorSystem internalActorSystem;
    private final ActorRefFactory actorRefFactory;

    public KafkaShardsThread(String clusterName, String bootstrapServers, InternalActorSystem internalActorSystem, ActorRefFactory actorRefFactory) {
        super("KafkaShardsThread-"+THREAD_ID_SEQUENCE.getAndIncrement());
        this.internalActorSystem = internalActorSystem;
        this.actorRefFactory = actorRefFactory;
        final Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfig.put("internal.leave.group.on.close", false);
        // MAX_POLL_INTERVAL_MS_CONFIG needs to be large for streams to handle cases when
        // streams is recovering data from state stores. We may set it to Integer.MAX_VALUE since
        // the streams code itself catches most exceptions and acts accordingly without needing
        // this timeout. Note however that deadlocks are not detected (by definition) so we
        // are losing the ability to detect them by setting this value to large. Hopefully
        // deadlocks happen very rarely or never.
        consumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED.name().toLowerCase(Locale.ROOT));
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, clusterName);
        consumerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG,internalActorSystem.getNode().getKey().getNodeId() + "-" + getName() + "-consumer");

        InternalMessageDeserializer internalMessageDeserializer = new InternalMessageDeserializer(new ActorRefDeserializer(actorRefFactory), internalActorSystem);
        consumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new KafkaInternalMessageDeserializer(internalMessageDeserializer));

        final Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // add client id with stream client id prefix
        producerConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, internalActorSystem.getNode().getKey().getNodeId() + "-" + getName() + "-producer");
    }
}
