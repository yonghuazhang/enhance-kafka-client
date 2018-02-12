package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.common.TopicPartition;

public class OrdinalConsumeHandlerContext extends AbstractConsumeHandlerContext {

    private boolean autoCommit;
    private long suspendTimeInMs;

    public OrdinalConsumeHandlerContext(TopicPartition tp, long ackOffset, int batchSize) {
        super(tp, ackOffset, batchSize);
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
