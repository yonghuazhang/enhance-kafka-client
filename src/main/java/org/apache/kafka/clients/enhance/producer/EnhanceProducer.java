package org.apache.kafka.clients.enhance.producer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.consumer.listener.ConsumeMessageHook;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class EnhanceProducer<K> extends KafkaProducer<K, ExtMessage<K>> implements ProduceOperator<K> {

    private KafkaProducer<K, ExtMessage<K>> innerProducer;
    private final ProducerClientContext<K> clientContext = new ProducerClientContext<>();

    public EnhanceProducer(Map<String, Object> configs) {

    }

    public EnhanceProducer(Properties properties) {
        super(properties);
    }

    @Override
    public String clientId() {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdownNow() {

    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) {

    }

    @Override
    public void suspend() {

    }

    @Override
    public void resume() {

    }

    @Override
    public List<PartitionInfo> partitionsForTopic(String topic) {
        return null;
    }

    @Override
    public void sendGroupOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String groupId) {

    }

    @Override
    public Future<RecordMetadata> sendMessage(ExtMessage<K> message) {
        return null;
    }

    @Override
    public void addProducerHook(ConsumeMessageHook consumeHook) {

    }
}
