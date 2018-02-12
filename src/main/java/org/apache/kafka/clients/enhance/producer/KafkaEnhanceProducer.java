package org.apache.kafka.clients.enhance.producer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.consumer.listener.ConsumeMessageHook;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaEnhanceProducer<K> implements ProduceOperator<K> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaEnhanceProducer.class);

    private final ProducerClientContext<K> clientContext = new ProducerClientContext<>();
    private KafkaProducerProxy<K> innerProducer;

    public KafkaEnhanceProducer(Map<String, Object> configs) {
        this(configs, null);
    }

    public KafkaEnhanceProducer(Map<String, Object> configs, Class<K> cls) {
        clientContext.producerConfig(configs);
        if (null != cls) {
            clientContext.keySerializer(Serdes.serdeFrom(cls).serializer());
        }
    }

    public KafkaEnhanceProducer(Properties properties) {
        this(properties, null);
    }

    public KafkaEnhanceProducer(Properties properties,  Class<K> cls) {
        clientContext.producerConfig(properties);
        if (null != cls) {
            clientContext.keySerializer(Serdes.serdeFrom(cls).serializer());
        }
    }

    @Override
    public void flush() {

    }

    @Override
    public List<PartitionInfo> partitionsForTopic(String topic) {
        return null;
    }

    @Override
    public void beginTransaction() {

    }

    @Override
    public void commitTransaction() {

    }

    @Override
    public void abortTransaction() {

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
}
