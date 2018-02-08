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
    private final ConcurrentConsumeHandlerContext handlerContext;
    private final ConcurrentMessageHandler<K> handler;
    private final long requestId = requestIdGenerator.incrementAndGet();
    private final String retryTopic;
    private final String deadletterTopic;

    public ConcurrentConsumeTaskRequest(AbsConsumeService<K> service, PartitionDataManager manager,
                                        List<ExtMessage<K>> extMessages, TopicPartition topicPartition,
                                        ConsumeClientContext<K> clientContext, ConcurrentMessageHandler<K> handler) {
        super(service, manager, extMessages, topicPartition, clientContext);
        long firstOffsetInBatch = messages.get(FIRST_MESSAGE_IDX).getOffset();
        this.handlerContext = new ConcurrentConsumeHandlerContext(topicPartition, firstOffsetInBatch, clientContext.consumeBatchSize());
        this.handler = handler;
        this.retryTopic = clientContext.retryTopicName();
        this.deadletterTopic = clientContext.deadLetterTopicName();
    }

    public long getRequestId() {
        return requestId;
    }

    @Override
    public void processConsumeStatus(ConsumeStatus status) {
        List<Long> offsets = new ArrayList<>(messages.size());
        switch (status) {
            case CONSUME_RETRY_LATER:
                List<ExtMessage<K>> failedRecords = new ArrayList<>();
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
                            boolean sendRetryOk = consumeService.sendMessageBack(retryTopic, msg, msg.getDelayedLevel());
                            if (!sendRetryOk) {
                                failedRecords.add(msg);
                            }
                        } else {
                            if (!consumeService.sendMessageBack(deadletterTopic, msg, 0)) {
                                logger.warn("sending dead letter message failed. please check it [{}].", msg.toString());
                            }
                        }
                    }
                }

                if (!failedRecords.isEmpty()) {
                    logger.trace("sending message back failed list:" + Arrays.toString(failedRecords.toArray(new Object[0])));
                    consumeService.dispatchTaskLater(new ConcurrentConsumeTaskRequest<>(consumeService, manager, failedRecords,
                            topicPartition, clientContext, handler), DelayedMessageTopic.SYS_DELAYED_TOPIC_10S.getDurationMs(), TimeUnit.MILLISECONDS);
                }

                for (ExtMessage<K> message : messages) {
                    if (failedRecords.isEmpty()) {
                        offsets.add(message.getOffset());
                    } else if (!failedRecords.contains(message)) {
                        offsets.add(message.getOffset());
                    }
                }
                logger.trace("start commitoffsets ---------------> " + offsets.toString());
                manager.commitSlidingWinOffset(topicPartition, offsets);
                break;
            case CONSUME_SUCCESS:
                for (ExtMessage<K> message : messages) {
                    offsets.add(message.getOffset());
                }
                logger.trace("start commitoffsets ---------------> " + offsets.toString());
                manager.commitSlidingWinOffset(topicPartition, offsets);
                break;
            default:
                break;
        }
    }

    @Override
    public ConsumeTaskResponse call() throws Exception {
        ConsumeStatus status = ConsumeStatus.CONSUME_RETRY_LATER;
        try {

            if (topicPartition.topic().equals(retryTopic)) {
                List<ExtMessage<K>> newMessages = getMessageFromRetryPartition(this.messages);
                status = handler.consumeMessage(newMessages, this.handlerContext);
            } else {
                status = handler.consumeMessage(this.messages, this.handlerContext);
            }
            processConsumeStatus(status);
        } catch (Throwable t) {
            logger.warn("user callback exec failed. due to ", t);
        }

        return null;
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

    private List<ExtMessage<K>> getMessageFromRetryPartition(List<ExtMessage<K>> messagesFromRetryPartition) {
        List<ExtMessage<K>> newMessages = new ArrayList<>(messagesFromRetryPartition.size());
        for (ExtMessage<K> message : messagesFromRetryPartition) {
            ExtMessage<K> newMessage = ExtMessage.parseFromRetryMessage(message);
            newMessages.add(newMessage);
        }
        return Collections.unmodifiableList(newMessages);
    }
}
