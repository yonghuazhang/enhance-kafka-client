package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.ExtMessageDef;
import org.apache.kafka.clients.enhance.ExtMessageUtils;
import org.apache.kafka.clients.producer.KafkaProducer;

import static org.apache.kafka.clients.enhance.ExtMessageDef.PROPERTY_REAL_OFFSET;
import static org.apache.kafka.clients.enhance.ExtMessageDef.PROPERTY_REAL_PARTITION_ID;
import static org.apache.kafka.clients.enhance.ExtMessageDef.PROPERTY_REAL_STORE_TIME;
import static org.apache.kafka.clients.enhance.ExtMessageDef.PROPERTY_REAL_TOPIC;

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
        return msg.getRetryCount() >= ExtMessageDef.MAX_RECONSUME_COUNT;
    }

    private void updateMessageAttrBeforeRetry(ExtMessage<K> msg, int delayedLevel) {

        msg.addProperty(PROPERTY_REAL_TOPIC, msg.getTopic());
        msg.addProperty(PROPERTY_REAL_PARTITION_ID, String.valueOf(msg.getPartion()));
        msg.addProperty(PROPERTY_REAL_OFFSET, String.valueOf(msg.getOffset()));
        msg.addProperty(PROPERTY_REAL_STORE_TIME, String.valueOf(msg.getStoreTimeMs()));
        //retry count + 1
        ExtMessageUtils.updateRetryCount(msg);
        ExtMessageUtils.setDelayedLevel(msg, delayedLevel);
    }

    private void updateMessageAttrAfterRetry(ExtMessage<K> msg) {
        if(msg.getRetryCount() > 0){
            ExtMessageUtils.setTopic(msg, msg.getProperty(PROPERTY_REAL_TOPIC));
            ExtMessageUtils.setPartion(msg, Integer.parseInt(msg.getProperty(PROPERTY_REAL_PARTITION_ID)));
            ExtMessageUtils.setOffset(msg, Integer.parseInt(msg.getProperty(PROPERTY_REAL_OFFSET)));
            ExtMessageUtils.setStoreTimeMs(msg, Long.parseLong(msg.getProperty(PROPERTY_REAL_STORE_TIME)));

            msg.removeProperty(PROPERTY_REAL_TOPIC);
            msg.removeProperty(PROPERTY_REAL_PARTITION_ID);
            msg.removeProperty(PROPERTY_REAL_OFFSET);
            msg.removeProperty(PROPERTY_REAL_STORE_TIME);
        }
    }
}
