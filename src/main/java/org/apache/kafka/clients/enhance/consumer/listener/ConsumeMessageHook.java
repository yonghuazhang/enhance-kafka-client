package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

public interface ConsumeMessageHook<K> extends ConsumerInterceptor<K, ExtMessage<K>> {
}
