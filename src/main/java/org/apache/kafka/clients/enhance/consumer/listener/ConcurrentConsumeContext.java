package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.clients.enhance.ExtMessageDef;
import org.apache.kafka.common.TopicPartition;

import java.util.BitSet;

public class ConcurrentConsumeContext extends AbstractConsumeContext {

    private int delayLevelAtReconsume = Integer.MIN_VALUE;

    private final BitSet batchConsumeFlags;

    public ConcurrentConsumeContext(TopicPartition tp, long beginOffset, int batchSize) {
        super(tp, beginOffset);
        this.batchConsumeFlags = new BitSet(batchSize);
    }

    public boolean isValidDelayLevel() {
        return delayLevelAtReconsume > 0 && delayLevelAtReconsume < ExtMessageDef.MAX_RECONSUME_COUNT;
    }

    public int getDelayLevelAtReconsume() {
        return delayLevelAtReconsume;
    }

    public void setDelayLevelAtReconsume(int delayLevelAtReconsume) {
        this.delayLevelAtReconsume = delayLevelAtReconsume;
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
