package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.producer.KafkaProducer;

public class OrdinalConsumeService<K> extends AbsConsumeService<K> {


    public OrdinalConsumeService(EnhanceConsumer<K> safeConsumer, KafkaProducer<K, ExtMessage<K>> innerSender, ConsumeClientContext<K> clientContext) {
        super(safeConsumer, innerSender, clientContext);
    }

    @Override
    public void start() {

    }

}
