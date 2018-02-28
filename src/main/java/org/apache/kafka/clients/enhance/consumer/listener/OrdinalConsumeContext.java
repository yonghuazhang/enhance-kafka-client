package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.common.TopicPartition;

public class OrdinalConsumeContext extends AbstractConsumeContext {
	private long suspendTimeInMs;

	public OrdinalConsumeContext(TopicPartition tp, long beginOffset) {
		super(tp, beginOffset);
		this.suspendTimeInMs = 3000L;
	}

	public long suspendTimeInMs() {
		return suspendTimeInMs;
	}

	public void suspendTimeInMs(long suspendTimeInMs) {
		this.suspendTimeInMs = suspendTimeInMs;
	}
}
