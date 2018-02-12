package org.apache.kafka.clients.enhance.producer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class EnhanceProducer<K> implements ProduceOperator<K> {

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
}
