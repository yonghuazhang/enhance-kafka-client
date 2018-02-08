package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.common.TopicPartition;

import java.util.BitSet;

public abstract class AbsConsumeHandlerContext {
    private final TopicPartition tp;
    private final long beginOffset;
    private final BitSet batchConsumeFlags;

    public AbsConsumeHandlerContext(TopicPartition tp, long beginOffset, int bitSize) {
        this.tp = tp;
        this.beginOffset = beginOffset;
        this.batchConsumeFlags = new BitSet(bitSize);
    }

    public TopicPartition getTopicPartition() {
        return tp;
    }

    public long getBeginOffset() {
        return beginOffset;
    }

    public void updateConsumeStatusInBatch(int pos, boolean consumeStatus) {
        synchronized (batchConsumeFlags) {
            batchConsumeFlags.set(pos, consumeStatus);
        }
    }

    public boolean getStatusByBatchIndex(int pos) {
        synchronized (batchConsumeFlags) {
            return batchConsumeFlags.get(pos);
        }
    }
}
