package org.apache.kafka.clients.enhance.producer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.producer.ProducerInterceptor;

public interface ProducerMessageHook<K> extends ProducerInterceptor<K, ExtMessage<K>> {
}
