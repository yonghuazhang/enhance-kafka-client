package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.enhance.ExtMessage;

public interface ConsumeMessageHook<K> extends ConsumerInterceptor<K, ExtMessage<K>> {
}
