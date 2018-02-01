package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.consumer.listener.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConsumeTaskRequest<K> implements Callable<ConsumeTaskResponse>, Delayed {
    private static final Logger logger = LoggerFactory.getLogger(ConsumeTaskRequest.class);
    private static final int FIRST_MESSAGE_IDX = 0;

    private final MessageHandler<K, ?> handler;
    private final ConsumeClientContext<K> clientContext;
    private final PartitionDataManager manager;
    private final List<ExtMessage<K>> messages;
    private final TopicPartition topicPartition;
    private final AbsConsumeService<K> consumeService;
    private final AbsConsumeHandlerContext messageHandlerContext;
    private final long taskCreatedTime = Time.SYSTEM.milliseconds();
    private volatile Future<ConsumeTaskResponse> taskResponseFuture;

    public ConsumeTaskRequest(AbsConsumeService<K> service, MessageHandler handler,
                              PartitionDataManager manager, List<ExtMessage<K>> messages,
                              TopicPartition topicPartition, ConsumeClientContext<K> clientContext) {
        this.consumeService = service;
        this.handler = handler;
        this.clientContext = clientContext;
        this.topicPartition = topicPartition;
        this.manager = manager;
        this.messages = Collections.unmodifiableList(messages);
        long firstOffsetInBatch = messages.get(FIRST_MESSAGE_IDX).getOffset();
        if (ConsumeType.CONSUME_ORDINAL == clientContext.consumeType()) {
            messageHandlerContext = new OrdinalConsumeHandlerContext(topicPartition, firstOffsetInBatch);
        } else {
            messageHandlerContext = new ConcurrentConsumeHandlerContext(topicPartition, firstOffsetInBatch);
        }
    }

    private void processConsumeStatus(ConsumeStatus status) {
        switch (status) {
            case CONSUME_RETRY_LATER:
                List<ExtMessage<K>> failedRecords = new ArrayList<>();
                for (ExtMessage<K> mesg : messages) {
                    boolean sendOk = consumeService.sendMessageBack(mesg, 1);
                    if (!sendOk) {
                        failedRecords.add(mesg);
                    }
                }
                if (!failedRecords.isEmpty()) {
                    logger.trace("sending message back failed list:" + Arrays.toString(failedRecords.toArray(new Object[0])));
                    consumeService.dispatchTaskLater(new ConsumeTaskRequest<>(handler, manager, failedRecords, topicPartition, pollService));
                }

                final List<Long> offsets = new ArrayList<>(messages.size());
                for (ExtMessage<K> message : messages) {
                    offsets.add(message.getOffset());
                }
                logger.trace("start commitoffsets ---------------> " + offsets.toString());
                manager.commitSlidingWinOffset(topicPartition, offsets);


                break;
            case CONSUME_SUCCESS:
                break;
            case CONSUME_DISCARD:
                break;
            default:

                break;
        }
    }

    @Override
    public ConsumeTaskResponse call() throws Exception {
        ConsumeStatus status = handler.consumeMessage(messages, );
        processConsumeStatus(status);
        return null;
    }

    public long getTaskCreatedTime() {
        return taskCreatedTime;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return Time.SYSTEM.milliseconds() - taskCreatedTime;
    }

    @Override
    public int compareTo(Delayed otherRequest) {
        return Long.compare(this.taskCreatedTime, ((ConsumeTaskRequest) otherRequest).getTaskCreatedTime());
    }

    public Future<ConsumeTaskResponse> getTaskResponseFuture() {
        return taskResponseFuture;
    }

    public void setTaskResponseFuture(Future<ConsumeTaskResponse> taskResponseFuture) {
        this.taskResponseFuture = taskResponseFuture;
    }
}
