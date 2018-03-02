package org.apache.kafka.clients.enhance.producer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.requests.MetadataResponse;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class KafkaEnhanceProducerTest {

    private KafkaEnhanceProducer<String> producer;

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void sendMessage() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.198.195.144:9092");
        props.put("acks", "all");
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        producer = new KafkaEnhanceProducer<>(props, String.class);
        producer.start();

        ExtMessage<String> message = new ExtMessage<>();
        message.setTopic("test");
        message.setMsgKey("aaaa");
        message.setMsgValue("helloworld".getBytes());
        Future<RecordMetadata> response = producer.sendMessage(message);
        RecordMetadata meta = response.get();
        System.out.println("---->" + meta);
    }

    @Test
    public void sendMessageCallback() throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.198.195.144:9092");
        props.put("acks", "all");

        producer = new KafkaEnhanceProducer<>(props, String.class);
        //producer.producerSetting().setTransactionId("test-1-1");
        producer.producerSetting().setIdempotence(true);
        producer.start();

        ExtMessage<String> message = new ExtMessage<>();
        message.setTopic("test");
        message.setMsgKey("aaac");
        message.setMsgValue("helloworld".getBytes());

        producer.beginTransaction();

        Future<RecordMetadata> response = producer.sendMessage(message);
        producer.commitTransaction();

        RecordMetadata meta = response.get();
        System.out.println("---->" + meta);
        producer.shutdownNow();
    }

    @Test
    public void sendWithHook() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.198.195.144:9092");
        props.put("acks", "all");
        //props.put("interceptor.classes","org.apache.kafka.clients.enhance.producer.TestPi");

        producer = new KafkaEnhanceProducer<>(props, String.class);
        //producer.producerSetting().setTransactionId("test-1-1");
        ///producer.producerSetting().setIdempotence(true);
        /*producer.addSendMessageHook(new SendMessageHook<String>() {
            @Override
            public ExtMessage<String> beforeSend(ExtMessage<String> message) {
                System.out.println("before -------->");
                return message;
            }

            @Override
            public void afterSend(RecordMetadata metadata, Exception exception) {
                System.out.println("after -------->" + metadata);

            }
        });*/
        producer.start();

        ExtMessage<String> message = new ExtMessage<>();
        message.setTopic("test2");
        message.setMsgKey("1");
        message.setMsgValue("helloworld".getBytes());



        for (int i = 0; i < 1; i++) {
            message.setTags(Integer.toString(i));
            producer.sendMessage(message).get();
        }

        /*producer.beginTransaction();
        Future<RecordMetadata> response = producer.sendMessage(message);
        RecordMetadata meta = response.get();
        *//*response = producer.sendMessage(message);
        response.get();*//*
        producer.commitTransaction();
        System.out.println("---->" + meta);*/
        producer.shutdownNow();

    }

}