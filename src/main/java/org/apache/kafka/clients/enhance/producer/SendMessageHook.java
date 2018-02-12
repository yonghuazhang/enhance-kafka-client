package org.apache.kafka.clients.enhance.producer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.producer.RecordMetadata;

public interface SendMessageHook<K> {

    ExtMessage<K> beforeSend(ExtMessage<K> message);

    void afterSend(RecordMetadata metadata, Exception exception);
}
