package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaFuture;

/**
 * Created by steven03.zhang on 2017/8/28.
 */
public class RetrieveRecordsResult<K, V> {
    private final KafkaFuture<ConsumerRecords<K, V>> future;

    public RetrieveRecordsResult(KafkaFuture<ConsumerRecords<K, V>> future) {
        this.future = future;
    }

    public KafkaFuture<ConsumerRecords<K, V>> values() {
        return future;
    }
}
