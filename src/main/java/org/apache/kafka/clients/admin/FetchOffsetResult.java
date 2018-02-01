package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;

/**
 * Created by steven03.zhang on 2017/9/11.
 */
public class FetchOffsetResult {
    private final KafkaFuture<Long> futures;

    public FetchOffsetResult(KafkaFuture<Long> futures) {
        this.futures = futures;
    }

    /**
     * Return a map from topic partition to futures which can be used to check the status of
     * individual topics.
     */
    public KafkaFuture<Long> values() {
        return futures;
    }
}
