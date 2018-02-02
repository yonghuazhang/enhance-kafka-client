package org.apache.kafka.clients.enhance;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class ExtMessageUtils {

    public static ExtMessage updateRetryCount(ExtMessage message) {
        message.updateRetryCount();
        return message;
    }

    public static ExtMessage setStoreTimeMs(ExtMessage message, long storeTimeMs) {
        message.setStoreTimeMs(storeTimeMs);
        return message;
    }

    public static ExtMessage setOffset(ExtMessage message, long offset) {
        message.setOffset(offset);
        return message;
    }

    public static ExtMessage setPartion(ExtMessage message, int partition) {
        message.setPartion(partition);
        return message;
    }

    public static ExtMessage setTopic(ExtMessage message, String topic) {
        message.setTopic(topic);
        return message;
    }

    public static ExtMessage clearProperty(ExtMessage message) {
        message.clearProperty();
        return message;
    }

    public static ExtMessage setDelayedLevel(ExtMessage message, int level) {
        message.setDelayedLevel(level);
        return message;
    }

    public static <K> ExtMessage<K> updateByRecord(ExtMessage message, ConsumerRecord<K, ExtMessage<K>> record) {
        message.updateByRecord(record);
        return message;
    }

}
