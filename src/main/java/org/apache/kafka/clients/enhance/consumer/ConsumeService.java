package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.enhance.AbsExtMessageFilter;
import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Created by steven03.zhang on 2017/12/13.
 */
public interface ConsumeService<K> {
    long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

    void start();

    void shutdown();

    void shutdownNow();

    void shutdown(long timeout, TimeUnit unit);

    void updateCoreThreadNum(int coreThreadNum);

    int getThreadCores();

    int getQueueSize();

    void submitConsumeRequest(final AbsConsumeTaskRequest<K> requestTask);

    boolean sendMessageBack(final String topic, final ExtMessage<K> msg, final int delayLevel);

    void seek(TopicPartition partition, long offset);

    void seekToTime(long timestamp);

    void seekToBeginning();

    void seekToEnd();

    ConsumerRebalanceListener getRebalanceListener();

    void subscribe(Collection<String> topics);

    void unsubscribe();

}
