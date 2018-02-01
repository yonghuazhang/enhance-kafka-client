package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.common.TopicPartition;

/**
 * Created by steven03.zhang on 2018/1/14.
 */
public class OrdinalConsumeHandlerContext extends AbsConsumeHandlerContext {

    private boolean autoCommit;
    private long suspendTimeInMs;

    public OrdinalConsumeHandlerContext(TopicPartition tp, long ackOffset) {
        super(tp, ackOffset);
        this.autoCommit = true;
        this.suspendTimeInMs = -1L;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public long getSuspendTimeInMs() {
        return suspendTimeInMs;
    }

    public void setSuspendTimeInMs(long suspendTimeInMs) {
        this.suspendTimeInMs = suspendTimeInMs;
    }
}
