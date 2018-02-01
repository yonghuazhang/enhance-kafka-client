package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.ExtMessageDef;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Created by steven03.zhang on 2017/12/13.
 */
public class ConcurrentConsumeService<K> extends AbsConsumeService<K> {

    private boolean retryTopicIsExists = false;
    private boolean deadLetterTopicIsExists = false;


    public ConcurrentConsumeService(ConsumerWithAdmin<K> safeConsumer, KafkaProducer<K, ExtMessage<K>> innerSender, ConsumeClientContext<K> clientContext) {
        super(safeConsumer, innerSender, clientContext);
        createRetryTopic();

    }

    @Override
    public void start() {

    }


    void createDeadLetterTopic() {
        String deadLetterTopic = clientContext.deadLetterTopicName();
        if (!deadLetterTopicIsExists) {
            deadLetterTopicIsExists = isTopicExists(deadLetterTopic);
        }

        if (!deadLetterTopicIsExists) {
            deadLetterTopicIsExists = safeConsumer.createTopic(deadLetterTopic);
        }
    }

    void createRetryTopic() {
        String retryTopic = clientContext.retryTopicName();
        if (!retryTopicIsExists) {
            retryTopicIsExists = isTopicExists(retryTopic);
        }

        if (!retryTopicIsExists) {
            retryTopicIsExists = safeConsumer.createTopic(retryTopic);
        }
    }

    boolean isTopicExists(String topic) {
        return safeConsumer.isTopicExists(topic);
    }

    private boolean needDeadLetterTopic(ExtMessage<K> msg) {
        return msg.getRetryTimes() >= ExtMessageDef.MAX_RECONSUME_TIMES;
    }

    private void updateMessageAttribute(ExtMessage<K> msg) {

    }
}
