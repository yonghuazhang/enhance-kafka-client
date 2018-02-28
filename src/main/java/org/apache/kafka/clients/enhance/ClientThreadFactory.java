package org.apache.kafka.clients.enhance;

import org.apache.kafka.common.utils.KafkaThread;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientThreadFactory implements ThreadFactory {
	private final static AtomicInteger THREAD_NUM_ACC = new AtomicInteger(0);
	private final String threadNamePrefix;

	public ClientThreadFactory() {
		threadNamePrefix = "kafka-client-thread";
	}

	public ClientThreadFactory(String threadNamePrefix) {
		this.threadNamePrefix = threadNamePrefix;
	}

	@Override
	public Thread newThread(Runnable r) {
		return KafkaThread.nonDaemon(threadNamePrefix + THREAD_NUM_ACC.incrementAndGet(), r);
	}
}
