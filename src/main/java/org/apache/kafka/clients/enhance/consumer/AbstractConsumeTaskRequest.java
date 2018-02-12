package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.consumer.listener.ConsumeStatus;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public abstract class AbstractConsumeTaskRequest<K> implements Callable<ConsumeTaskResponse>, Delayed {
    protected static final Logger logger = LoggerFactory.getLogger(AbstractConsumeTaskRequest.class);
    protected static final int FIRST_MESSAGE_IDX = 0;

    protected final ConsumeClientContext<K> clientContext;
    protected final PartitionDataManager manager;
    protected final List<ExtMessage<K>> messages;
    protected final TopicPartition topicPartition;
    protected final AbstractConsumeService<K> consumeService;
    protected final long taskCreatedTime = Time.SYSTEM.milliseconds();
    protected volatile Future<ConsumeTaskResponse> taskResponseFuture;

    public AbstractConsumeTaskRequest(AbstractConsumeService<K> service, PartitionDataManager manager,
                                      List<ExtMessage<K>> messages, TopicPartition topicPartition,
                                      ConsumeClientContext<K> clientContext) {
        this.consumeService = service;
        this.clientContext = clientContext;
        this.topicPartition = topicPartition;
        this.manager = manager;
        this.messages = Collections.unmodifiableList(messages);
    }

    public abstract void processConsumeStatus(ConsumeStatus status);

    public long getTaskCreatedTime() {
        return taskCreatedTime;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return Time.SYSTEM.milliseconds() - taskCreatedTime;
    }

    @Override
    public int compareTo(Delayed otherRequest) {
        return Long.compare(this.taskCreatedTime, ((AbstractConsumeTaskRequest) otherRequest).getTaskCreatedTime());
    }

    public Future<ConsumeTaskResponse> getTaskResponseFuture() {
        return taskResponseFuture;
    }

    public void setTaskResponseFuture(Future<ConsumeTaskResponse> taskResponseFuture) {
        this.taskResponseFuture = taskResponseFuture;
    }
}
