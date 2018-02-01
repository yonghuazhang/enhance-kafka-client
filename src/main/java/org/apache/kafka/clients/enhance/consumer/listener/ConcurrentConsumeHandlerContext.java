package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.common.TopicPartition;

/**
 * Created by steven03.zhang on 2017/12/14.
 */
public class ConcurrentConsumeHandlerContext extends AbsConsumeHandlerContext {

    private int delayLevelAtReconsume;

    public ConcurrentConsumeHandlerContext(TopicPartition tp, long ackOffset) {
        super(tp, ackOffset);
        this.delayLevelAtReconsume = 0;
    }

    public int getDelayLevelAtReconsume() {
        return delayLevelAtReconsume;
    }

    public void setDelayLevelAtReconsume(int delayLevelAtReconsume) {
        this.delayLevelAtReconsume = delayLevelAtReconsume;
    }
}
