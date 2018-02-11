package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.ShutdownableThread;
import org.apache.kafka.clients.enhance.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConcurrentConsumeService<K> extends AbsConsumeService<K> {
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentConsumeService.class);

    private final ConcurrentHashMap<Long, ConcurrentConsumeTaskRequest<K>> requestMap = new ConcurrentHashMap<>();
    private final Timer expiredTimer = new Timer("task-request-expired-timer");
    private boolean retryTopicIsExists = false;
    private boolean deadLetterTopicIsExists = false;


    public ConcurrentConsumeService(EnhanceConsumer<K> safeConsumer, KafkaProducer<K, ExtMessage<K>> innerSender, ConsumeClientContext<K> clientContext) {
        super(safeConsumer, innerSender, clientContext);
        this.dispatchService = new ConcurrentDispatchMessageService("concurrent-dispatch-message-service-thread");
        switch (clientContext.consumeModel()) {
            case GROUP_CLUSTERING:
                createRetryTopic();
                break;
            case GROUP_BROADCASTING:
            default:
                break;
        }
    }


    public class ConcurrentDispatchMessageService extends ShutdownableThread {

        public ConcurrentDispatchMessageService(String name) {
            super(name);
        }

        @Override
        public void doWork() {
            while (isRunning) {
                Set<TopicPartition> topicPartitions = partitionDataManager.getAssignedPartition();
                if (null != topicPartitions && !topicPartitions.isEmpty()) {
                    for (TopicPartition topicPartition : topicPartitions) {
                        List<ConsumerRecord<K, ExtMessage<K>>> records =
                                partitionDataManager.retrieveTaskRecords(topicPartition, clientContext.consumeBatchSize());
                        if (!records.isEmpty()) {
                            List<ExtMessage<K>> messages = new ArrayList<>(records.size());
                            for (ConsumerRecord<K, ExtMessage<K>> record : records) {
                                messages.add(record.value());
                            }
                            ConcurrentConsumeTaskRequest<K> requestTask = new ConcurrentConsumeTaskRequest<>(ConcurrentConsumeService.this, partitionDataManager,
                                    messages, topicPartition, clientContext);
                            logger.debug("[ConcurrentDispatchMessageService] dispatch consuming task at once. messages = " + messages);
                            submitConsumeRequest(requestTask);
                            requestMap.put(requestTask.getRequestId(), requestTask);
                        }
                    }
                } else { // not assigned any partition, standby service
                    Utility.sleep(clientContext.pollMessageAwaitTimeoutMs());
                }
            }
        }
    }

    ConcurrentConsumeTaskRequest<K> removeCompletedTask(long taskRequestId) {
        logger.debug("[ConcurrentConsumeService] remove completed task [taskId = {}].", taskRequestId);
        return requestMap.remove(taskRequestId);
    }

    class processExpiredTaskRequest extends TimerTask {

        @Override
        public void run() {
            Iterator<ConcurrentConsumeTaskRequest<K>> requestItor = requestMap.values().iterator();
            while (requestItor.hasNext()) {
                ConcurrentConsumeTaskRequest<K> taskRequest = requestItor.next();
                Future<ConsumeTaskResponse> responseFuture = taskRequest.getTaskResponseFuture();
                if (responseFuture.isDone()) {
                    requestMap.remove(taskRequest.getRequestId());
                } else {
                    if (taskRequest.getDelay(TimeUnit.MILLISECONDS) > clientContext.maxMessageDealTimeMs()) {
                        responseFuture.cancel(true);
                    }
                }
            }
        }
    }

    @Override
    public void subscribe(Collection<String> topics) {
        List<String> subTopics = new ArrayList<>(topics);
        switch (clientContext.consumeModel()) {
            case GROUP_CLUSTERING:
                subTopics.add(clientContext.retryTopicName());
                break;
            case GROUP_BROADCASTING:
            default:
                break;
        }
        super.subscribe(subTopics);
    }

    @Override
    public void start() {
        try {
            super.start();
            expiredTimer.scheduleAtFixedRate(new processExpiredTaskRequest(), clientContext.maxMessageDealTimeMs(),
                    clientContext.maxMessageDealTimeMs());
            logger.info("[ConcurrentConsumeService] start successfully.");
        } catch (Exception ex) {
            logger.warn("[ConcurrentConsumeService] service failed to start. due to ", ex);
            shutdown(0, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) {
        requestMap.clear();
        expiredTimer.cancel();
        super.shutdown(timeout, unit);
    }

    void createDeadLetterTopic() {
        if (deadLetterTopicIsExists) return;

        String deadLetterTopic = clientContext.deadLetterTopicName();
        if (!deadLetterTopicIsExists) {
            deadLetterTopicIsExists = isTopicExists(deadLetterTopic);
        }

        if (!deadLetterTopicIsExists) {
            deadLetterTopicIsExists = safeConsumer.createTopic(deadLetterTopic);
        }
    }

    void createRetryTopic() {
        if (retryTopicIsExists) return;

        String retryTopic = clientContext.retryTopicName();
        if (!retryTopicIsExists) {
            retryTopicIsExists = isTopicExists(retryTopic);
        }

        if (!retryTopicIsExists) {
            retryTopicIsExists = safeConsumer.createTopic(retryTopic);
        }
    }

    boolean isTopicExists(String topic) {
        return safeConsumer.isTopicExists(topic);
    }
}
