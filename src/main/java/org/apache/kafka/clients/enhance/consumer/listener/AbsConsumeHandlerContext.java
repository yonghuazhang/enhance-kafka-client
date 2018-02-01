package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.common.TopicPartition;

public abstract class AbsConsumeHandlerContext {
    private final TopicPartition tp;
    private final long beginOffset;
    private int cursor = 0;

    public AbsConsumeHandlerContext(TopicPartition tp, long beginOffset) {
        this.tp = tp;
        this.beginOffset = beginOffset;
    }

    public TopicPartition getTopicPartition() {
        return tp;
    }

    public long getBeginOffset() {
        return beginOffset;
    }

    public void updateCursorInBatch() {
        cursor++;
    }

    public int getCursor() {
        return cursor;
    }
}
