package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.consumer.listener.AbsConsumeHandlerContext;
import org.apache.kafka.clients.enhance.consumer.listener.ConcurrentConsumeHandlerContext;
import org.apache.kafka.clients.enhance.consumer.listener.ConcurrentMessageHandler;
import org.apache.kafka.clients.enhance.consumer.listener.ConsumeStatus;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class KafkaPushConsumerTest {
    private KafkaPushConsumer<String> consumer;

    @Before
    public void setUp() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.198.195.144:9092");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("group.id", "test_zzz_1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        //props.put("buffer.memory", 33554432);
        //props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        consumer = new KafkaPushConsumer<String>(props, String.class);
        consumer.consumeSetting().consumeBatchSize(10);
        consumer.registerHandler(new ConcurrentMessageHandler<String>() {
            @Override
            public ConsumeStatus consumeMessage(List<ExtMessage<String>> message, ConcurrentConsumeHandlerContext consumeContext) {
                return null;
            }
        });



    }

    @Test
    public void start() throws Exception {
        consumer.subscribe("test");
        consumer.start();

        TimeUnit.SECONDS.sleep(1000);
    }

}