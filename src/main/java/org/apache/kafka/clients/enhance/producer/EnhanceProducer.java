package org.apache.kafka.clients.enhance.producer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;
import java.util.Properties;

public class EnhanceProducer<K> implements ProduceOperator<K> {

    private KafkaProducer<K, ExtMessage<K>> innerProducer;
    private final ProducerClientContext<K> clientContext = new ProducerClientContext<>();

    public EnhanceProducer(Map<String, Object> configs) {
        super(configs);
    }

    public EnhanceProducer(Properties properties) {
        super(properties);
    }
}
