package org.apache.kafka.clients.enhance;

import org.apache.kafka.common.utils.KafkaThread;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientThreadFactory implements ThreadFactory {
    private final static AtomicInteger THREAD_NUM_ACC = new AtomicInteger(0);
    private final String threadNamePreffix;

    public ClientThreadFactory() {
        threadNamePreffix = "kafka-client-thread";
    }

    public ClientThreadFactory(String threadNamePreffix) {
        this.threadNamePreffix = threadNamePreffix;
    }

    @Override
    public Thread newThread(Runnable r) {
        return KafkaThread.nonDaemon(threadNamePreffix + THREAD_NUM_ACC.incrementAndGet(), r);
    }
}
