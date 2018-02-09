package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.ExtMessageUtils;
import org.apache.kafka.clients.enhance.consumer.listener.ConcurrentConsumeHandlerContext;
import org.apache.kafka.clients.enhance.consumer.listener.ConcurrentMessageHandler;
import org.apache.kafka.clients.enhance.consumer.listener.ConsumeStatus;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.clients.enhance.ExtMessageDef.MAX_RECONSUME_COUNT;
import static org.apache.kafka.clients.enhance.ExtMessageDef.PROPERTY_REAL_OFFSET;
import static org.apache.kafka.clients.enhance.ExtMessageDef.PROPERTY_REAL_PARTITION_ID;
import static org.apache.kafka.clients.enhance.ExtMessageDef.PROPERTY_REAL_STORE_TIME;
import static org.apache.kafka.clients.enhance.ExtMessageDef.PROPERTY_REAL_TOPIC;

public class ConcurrentConsumeTaskRequest<K> extends AbsConsumeTaskRequest<K> {
    private static final AtomicLong requestIdGenerator = new AtomicLong(0L);
    private final long requestId = requestIdGenerator.incrementAndGet();
    private final ConcurrentConsumeHandlerContext handlerContext;
    private final ConcurrentMessageHandler<K> handler;
    private final String retryTopic;
    private final String deadletterTopic;

    public ConcurrentConsumeTaskRequest(AbsConsumeService<K> service, PartitionDataManager manager,
                                        List<ExtMessage<K>> extMessages, TopicPartition topicPartition,
                                        ConsumeClientContext<K> clientContext) {
        super(service, manager, extMessages, topicPartition, clientContext);
        long firstOffsetInBatch = messages.get(FIRST_MESSAGE_IDX).getOffset();
        this.handlerContext = new ConcurrentConsumeHandlerContext(topicPartition, firstOffsetInBatch, clientContext.consumeBatchSize());
        this.handler = (ConcurrentMessageHandler<K>) clientContext.messageHandler();
        this.retryTopic = clientContext.retryTopicName();
        this.deadletterTopic = clientContext.deadLetterTopicName();
    }

    public long getRequestId() {
        return requestId;
    }

    private ConcurrentConsumeService<K> getConsumeService() {
        return (ConcurrentConsumeService<K>) this.consumeService;
    }

    @Override
    public void processConsumeStatus(ConsumeStatus status) {
        List<Long> offsets = new ArrayList<>(messages.size());
        switch (status) {
            case CONSUME_RETRY_LATER:
                List<ExtMessage<K>> localRetryRecords = new ArrayList<>();
                int messageSize = messages.size();

                for (int idx = 0; idx < messageSize; idx++) {
                    if (!handlerContext.getStatusByBatchIndex(idx)) {
                        ExtMessage<K> msg = messages.get(idx);
                        int delayLevel = msg.getRetryCount() + 1;
                        if (handlerContext.isValidDelayLevel()) {
                            delayLevel = handlerContext.getDelayLevelAtReconsume();
                        }

                        if (msg.getRetryCount() < MAX_RECONSUME_COUNT) {
                            updateMessageAttrBeforeRetrySendback(msg, delayLevel);

                            boolean isLocalRetry = false;
                            switch (clientContext.consumeModel()) {
                                case GROUP_CLUSTERING:
                                    isLocalRetry = consumeService.sendMessageBack(retryTopic, msg, msg.getDelayedLevel());
                                    break;
                                case GROUP_BROADCASTING:
                                default:
                                    break;
                            }

                            if (!isLocalRetry) {
                                localRetryRecords.add(msg);
                            }
                        } else {
                            switch (clientContext.consumeModel()) {
                                case GROUP_CLUSTERING:
                                    ((ConcurrentConsumeService) consumeService).createDeadLetterTopic();
                                    if (!consumeService.sendMessageBack(deadletterTopic, msg, 0)) {
                                        logger.warn("sending dead letter message failed. please check it [{}].", msg);
                                    }
                                    break;
                                case GROUP_BROADCASTING:
                                default:
                                    logger.warn("[ConcurrentConsumeTaskRequest-Broadcast] message [{}] will be dropped, since exceed max retry count.", msg);
                                    break;
                            }

                        }
                    }
                }

                if (!localRetryRecords.isEmpty()) {
                    logger.trace("need local retry message list:" + Arrays.toString(localRetryRecords.toArray(new Object[0])));
                    consumeService.dispatchTaskLater(new ConcurrentConsumeTaskRequest<>(consumeService, manager, localRetryRecords,
                            topicPartition, clientContext), DelayedMessageTopic.SYS_DELAYED_TOPIC_5S.getDurationMs(), TimeUnit.MILLISECONDS);
                }

                for (ExtMessage<K> message : messages) {
                    if (localRetryRecords.isEmpty()) {
                        offsets.add(message.getOffset());
                    } else if (!localRetryRecords.contains(message)) {
                        offsets.add(message.getOffset());
                    }
                }
                logger.trace("start commitoffsets ---------------> " + offsets);
                manager.commitOffsets(topicPartition, offsets);
                break;
            case CONSUME_SUCCESS:
                for (ExtMessage<K> message : messages) {
                    offsets.add(message.getOffset());
                }
                logger.trace("start commitoffsets ---------------> " + offsets);
                manager.commitOffsets(topicPartition, offsets);
                break;
            default:
                logger.warn("unknown ConsumeStatus. offset = " + offsets);
                manager.commitOffsets(topicPartition, offsets);
                break;
        }
        getConsumeService().removeCompletedTask(requestId);
    }

    @Override
    public ConsumeTaskResponse call() throws Exception {
        ConsumeStatus status = ConsumeStatus.CONSUME_RETRY_LATER;
        try {
            if (topicPartition.topic().equals(retryTopic)) {
                List<ExtMessage<K>> newMessages = fetchMessagesFromRetryPartition(this.messages);
                status = handler.consumeMessage(newMessages, this.handlerContext);
            } else {
                status = handler.consumeMessage(this.messages, this.handlerContext);
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

    private void updateMessageAttrBeforeRetrySendback(ExtMessage<K> msg, int delayedLevel) {
        if (msg.getRetryCount() == 0) {
            msg.addProperty(PROPERTY_REAL_TOPIC, msg.getTopic());
            msg.addProperty(PROPERTY_REAL_PARTITION_ID, String.valueOf(msg.getPartion()));
            msg.addProperty(PROPERTY_REAL_OFFSET, String.valueOf(msg.getOffset()));
            msg.addProperty(PROPERTY_REAL_STORE_TIME, String.valueOf(msg.getStoreTimeMs()));
        }
        //retry count + 1
        ExtMessageUtils.updateRetryCount(msg);
        ExtMessageUtils.setDelayedLevel(msg, delayedLevel);
    }

    private List<ExtMessage<K>> fetchMessagesFromRetryPartition(List<ExtMessage<K>> messagesFromRetryPartition) {
        List<ExtMessage<K>> newMessages = new ArrayList<>(messagesFromRetryPartition.size());
        for (ExtMessage<K> message : messagesFromRetryPartition) {
            ExtMessage<K> newMessage = ExtMessage.parseFromRetryMessage(message);
            newMessages.add(newMessage);
        }
        return Collections.unmodifiableList(newMessages);
    }
}
