package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.common.TopicPartition;

public class ConsumeHookContext extends AbsConsumeHandlerContext {
    public ConsumeHookContext(TopicPartition tp, long ackOffset, int batchSize) {
        super(tp, ackOffset, batchSize);
    }
}
