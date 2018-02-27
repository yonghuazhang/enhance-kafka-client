package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.consumer.listener.ConsumeStatus;
import org.apache.kafka.clients.enhance.consumer.listener.OrdinalConsumeContext;
import org.apache.kafka.clients.enhance.consumer.listener.OrdinalMessageHandler;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class OrdinalConsumeTaskRequest<K> extends AbstractConsumeTaskRequest<K> {
    private final OrdinalConsumeContext handlerContext;
    private final OrdinalMessageHandler<K> handler;
    private volatile boolean isShutdownTask = false;

    public OrdinalConsumeTaskRequest(AbstractConsumeService<K> service, PartitionDataManager manager,
                                     List<ExtMessage<K>> extMessages, TopicPartition topicPartition,
                                     ConsumeClientContext<K> clientContext) {
        super(service, manager, extMessages, topicPartition, clientContext);
        long firstOffsetInBatch = messages.get(FIRST_MESSAGE_IDX).getOffset();
        this.handlerContext = new OrdinalConsumeContext(topicPartition, firstOffsetInBatch);
        this.handler = (OrdinalMessageHandler<K>) clientContext.messageHandler();
    }

    private OrdinalConsumeService<K> ordinalConsumeService() {
        return (OrdinalConsumeService<K>) this.consumeService;
    }

    public void setShutdownTask(boolean shutdownTask) {
        isShutdownTask = shutdownTask;
    }

    @Override
    public void processConsumeStatus(ConsumeStatus status) {
        List<Long> offsets = new ArrayList<>(messages.size());
        switch (status) {
            case CONSUME_RETRY_LATER:
                if (!isShutdownTask) {
                    //retry consuming the messages after suspend time. modified the suspending time.
                    if (handlerContext.suspendTimeInMs() < DelayedMessageTopic.SYS_DELAYED_TOPIC_5S.getDurationMs()) {
                        handlerContext.suspendTimeInMs(DelayedMessageTopic.SYS_DELAYED_TOPIC_5S.getDurationMs());
                    } else if (handlerContext.suspendTimeInMs() > DelayedMessageTopic.SYS_DELAYED_TOPIC_2H.getDurationMs()) {
                        handlerContext.suspendTimeInMs(DelayedMessageTopic.SYS_DELAYED_TOPIC_2H.getDurationMs());
                    }

                    consumeService.dispatchTaskLater(this, handlerContext.suspendTimeInMs(), TimeUnit.MILLISECONDS);
                }
                break;
            case CONSUME_SUCCESS:
                try {
                    for (ExtMessage<K> message : messages) {
                        offsets.add(message.getOffset());
                    }
                    logger.trace("start commitoffsets ---------------> " + offsets);
                    manager.commitOffsets(topicPartition, offsets);
                } finally {
                    ordinalConsumeService().getConsumeTasks().remove(topicPartition);
                }
                break;
            default:
                logger.warn("unknown ConsumeStatus. offset = " + offsets);
                try {
                    for (ExtMessage<K> message : messages) {
                        offsets.add(message.getOffset());
                    }
                    manager.commitOffsets(topicPartition, offsets);
                } finally {
                    ordinalConsumeService().getConsumeTasks().remove(topicPartition);
                }
                break;
        }
    }

    @Override
    public ConsumeTaskResponse call() throws Exception {
        if (isShutdownTask) return ConsumeTaskResponse.TASK_EXEC_FAILURE;
        ConsumeStatus status = ConsumeStatus.CONSUME_RETRY_LATER;
        try {
            status = handler.consumeMessage(this.messages, this.handlerContext);
            if (null == status) {
                logger.warn("consuming handler return null status, status will be replaced by [CONSUME_RETRY_LATER].");
                status = ConsumeStatus.CONSUME_RETRY_LATER;
            }
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                logger.info("[ConcurrentConsumeTaskRequest] callback exec too long(>{}ms), interrupted the task.", clientContext.maxMessageDealTimeMs());
            } else {
                logger.warn("[ConcurrentConsumeTaskRequest] callback execute failed. due to ", t);
            }
            return ConsumeTaskResponse.TASK_EXEC_FAILURE;
        } finally {
            processConsumeStatus(status);
        }
        return ConsumeTaskResponse.TASK_EXEC_SUCCESS;
    }
}
