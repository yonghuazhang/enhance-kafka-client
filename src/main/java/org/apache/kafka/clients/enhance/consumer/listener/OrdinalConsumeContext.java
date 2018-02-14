package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.common.TopicPartition;

public class OrdinalConsumeContext extends AbstractConsumeContext {

    private boolean autoCommit;
    private long suspendTimeInMs;

    public OrdinalConsumeContext(TopicPartition tp, long ackOffset) {
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
