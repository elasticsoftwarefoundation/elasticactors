package org.elasticsoftware.elasticactors.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;

public final class KafkaTransactionContext {
    private static final ThreadLocal<KafkaProducer<Object, Object>> transactionalProducer = new ThreadLocal<>();

    static void setTransactionalProducer(KafkaProducer<Object, Object> producer) {
        transactionalProducer.set(producer);
    }

    static KafkaProducer<Object, Object> getProducer() {
        return transactionalProducer.get();
    }

    static void clear() {
        transactionalProducer.set(null);
    }
}
