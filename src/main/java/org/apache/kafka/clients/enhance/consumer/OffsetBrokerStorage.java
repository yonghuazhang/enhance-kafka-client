package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.enhance.exception.ConsumeOffsetException;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

/**
 * Created by steven03.zhang on 2017/12/13.
 */
public class OffsetBrokerStorage<K> extends AbsOffsetStorage<K> {


    protected OffsetBrokerStorage(ConsumerWithAdmin<K> safeConsumer, PartitionDataManager partitionDataManager, ConsumeClientContext clientContext) {
        super(safeConsumer, partitionDataManager, clientContext, LoadOffsetType.LOAD_FROM_BROKER);
    }

    @Override
    public boolean load() {
        return true;
    }

    @Override
    public void storeOffsetMeta(Map<TopicPartition, OffsetAndMetadata> ackOffsetMeta) throws ConsumeOffsetException {

        if (null == ackOffsetMeta || ackOffsetMeta.isEmpty()) return;

        try {
            this.safeConsumer.commitSync(ackOffsetMeta);
        } catch (Exception e) {
            logger.warn("[storeOffsetMeta()] error. due to ", e);
            throw new ConsumeOffsetException("kafka consumer commitSync failed.", e);
        }
    }
}
