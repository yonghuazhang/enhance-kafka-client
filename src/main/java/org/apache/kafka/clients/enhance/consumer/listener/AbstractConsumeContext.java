package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.common.TopicPartition;

import java.util.BitSet;

public abstract class AbstractConsumeContext {
    private final TopicPartition tp;
    private final long beginOffset;


    public AbstractConsumeContext(TopicPartition tp, long beginOffset) {
        this.tp = tp;
        this.beginOffset = beginOffset;

    }

    public TopicPartition getTopicPartition() {
        return tp;
    }

    public long getBeginOffset() {
        return beginOffset;
    }

}
