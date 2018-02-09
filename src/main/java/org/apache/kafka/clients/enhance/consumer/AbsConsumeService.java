package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.enhance.ClientThreadFactory;
import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.ShutdownableThread;
import org.apache.kafka.clients.enhance.exception.KafkaConsumeException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbsConsumeService<K> implements ConsumeService<K> {
    protected static final Logger logger = LoggerFactory.getLogger(AbsConsumeService.class);
    protected static final long SEND_MESSAGE_BACK_WAIT_TIMEOUT_MS = 3000L;

    private final ThreadPoolExecutor execTaskService;
    private final ArrayBlockingQueue<Runnable> taskQueue;
    private final ScheduledExecutorService scheduleExecTaskService;
    protected final ClientThreadFactory clientThreadFactory = new ClientThreadFactory("consume-service-thread-pool");
    protected final KafkaPollMessageService<K> pollService;
    protected ShutdownableThread dispatchService;

    protected final ReentrantLock syncLock = new ReentrantLock(true);

    protected final AbsOffsetStorage offsetPersistor;
    protected final ConsumerWithAdmin<K> safeConsumer;
    protected final KafkaProducer<K, ExtMessage<K>> innerSender;
    protected final PartitionDataManager<K, ExtMessage<K>> partitionDataManager;

    protected final ConsumeClientContext<K> clientContext;
    protected volatile boolean isRunning = false;

    public AbsConsumeService(final ConsumerWithAdmin<K> safeConsumer,
                             final KafkaProducer<K, ExtMessage<K>> innerSender,
                             final ConsumeClientContext<K> clientContext) {
        this.safeConsumer = safeConsumer;
        this.innerSender = innerSender;
        this.clientContext = clientContext;
        this.partitionDataManager = new PartitionDataManager<>();
        this.pollService = new KafkaPollMessageService("kafka-poll-message-service", safeConsumer,
                partitionDataManager, clientContext, syncLock);
        this.taskQueue = new ArrayBlockingQueue<>(clientContext.consumeQueueSize(), true);
        //[default resetStrategy] is RejectedStrategy, need process RejectedException.
        int coreThreadNum = clientContext.consumeThreadNum();
        this.execTaskService = new ThreadPoolExecutor(coreThreadNum, coreThreadNum << 1L,
                1000 * 15,//
                TimeUnit.MILLISECONDS,
                this.taskQueue,
                clientThreadFactory
        );

        this.scheduleExecTaskService = Executors.newSingleThreadScheduledExecutor(clientThreadFactory);

        switch (clientContext.consumeModel()) {
            case GROUP_BROADCASTING:
                this.offsetPersistor = new OffsetFileStorage(safeConsumer, partitionDataManager, clientContext);

                break;
            case GROUP_CLUSTERING:
            case NO_CONSUMER_MODEL:
            default:
                this.offsetPersistor = new OffsetBrokerStorage(safeConsumer, partitionDataManager, clientContext);
                break;
        }

    }

    @Override
    public void start() {
        logger.debug("[AbsConsumeService] start service.");
        syncLock.lock();
        try {
            if (!isRunning) {
                subscribe(clientContext.getTopics());
                pollService.start();
                dispatchService.start();
                offsetPersistor.start();
                isRunning = true;
            }
        } catch (Exception ex) {
            shutdownNow();
            throw new KafkaConsumeException("start consumeService failed.");
        } finally {
            syncLock.unlock();
        }


    }

    @Override
    public void shutdown() {
        shutdown(DEFAULT_CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdownNow() {
        shutdown(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) {
        logger.debug("[AbsConsumeService] start closing service.");
        syncLock.lock();
        try {
            if (isRunning) {
                isRunning = false;
                pollService.shutdown();
                dispatchService.shutdown();

                this.taskQueue.clear();
                if (0 >= timeout) {
                    logger.debug("[AbsConsumeService] is isRunning at once.");
                    this.execTaskService.shutdownNow();
                    this.scheduleExecTaskService.shutdownNow();
                } else {
                    logger.debug("[AbsConsumeService] awaitTermination for [{}] ms.", unit.toMillis(timeout));
                    this.execTaskService.shutdown();
                    this.scheduleExecTaskService.shutdown();
                    try {
                        this.execTaskService.awaitTermination(timeout, unit);
                        this.scheduleExecTaskService.awaitTermination(timeout, unit);
                    } catch (InterruptedException e) {
                        logger.warn("[AbsConsumeService] interrupted exception. due to ", e);
                    }
                }

                offsetPersistor.shutdown();
            }
        } catch (Throwable e) {
            logger.warn("[AbsConsumeService] close error. due to ", e);
        } finally {
            syncLock.unlock();
        }
    }

    @Override
    public void updateCoreThreadNum(int coreThreadNum) {
        try {
            execTaskService.setCorePoolSize(coreThreadNum);
        } catch (IllegalArgumentException e) {
            logger.warn("[AbsConsumeService] update consuming thread-pool coreThread error. due to ", e);
        }
    }

    @Override
    public int getThreadCores() {
        return execTaskService.getCorePoolSize();
    }

    @Override
    public int getQueueSize() {
        return this.taskQueue.size();
    }

    @Override
    public void submitConsumeRequest(AbsConsumeTaskRequest<K> requestTask) {
        try {
            dispatchTaskAtOnce(requestTask);
        } catch (RejectedExecutionException e) {
            logger.warn("[AbsConsumeService-submitConsumeRequest] task is too much. and wait 3s and dispatch again.");
            dispatchTaskLater(requestTask, clientContext.clientRetryBackoffMs(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public boolean sendMessageBack(String topic, ExtMessage<K> msg, int delayLevel) {
        ProducerRecord<K, ExtMessage<K>> record = null;
        try {
            record = new ProducerRecord<>(topic, msg.getMsgKey(), msg);
            Future<RecordMetadata> result = innerSender.send(record);
            result.get(SEND_MESSAGE_BACK_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            logger.trace("sendMessageBack message[{}] successfully.", record.toString());
            return true;
        } catch (Throwable e) {
            logger.warn("sendMessageBack failed. records = [{}]", record);
        }
        return false;
    }

    private Set<TopicPartition> filterAssignedRetryTopic(Set<TopicPartition> origAssigned) {
        Set<TopicPartition> filterTopicPartitions = new HashSet<>();
        for(TopicPartition tp : origAssigned) {
            if (!tp.topic().equals(clientContext.retryTopicName())) {
                filterTopicPartitions.add(tp);
            }
        }
        return filterTopicPartitions;
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        if (isRunning) {
            logger.info("TopicPartition[{}] seek to [{}].", partition, offset);
            syncLock.lock();
            try {
                safeConsumer.seek(partition, offset);
                partitionDataManager.resetPartitionData(partition);
            } catch (Exception ex) {
                logger.warn("seek offset error. due to ", ex);
            } finally {
                syncLock.unlock();
            }
        } else {
            logger.info("Consume service hasn't been initialized.");
        }
    }

    @Override
    public void seekToTime(long timestamp) {
        if (isRunning) {
            syncLock.lock();
            try {
                Set<TopicPartition> tps = filterAssignedRetryTopic(safeConsumer.assignment());
                HashMap<TopicPartition, Long> searchByTimestamp = new HashMap<>();

                for (TopicPartition tp : tps) {
                    searchByTimestamp.put(tp, timestamp);
                }

                for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : safeConsumer.offsetsForTimes(searchByTimestamp).entrySet()) {
                    safeConsumer.seek(entry.getKey(), entry.getValue().offset());
                }
                partitionDataManager.resetAllPartitionData();

            } catch (Exception ex) {
                logger.warn("seekToTime error. due to ", ex);
            } finally {
                syncLock.unlock();
            }
        } else {
            logger.info("Consume service hasn't been initialized.");
        }
    }

    @Override
    public void seekToBeginning() {
        if (isRunning) {
            syncLock.lock();
            try {
                safeConsumer.seekToBeginning(filterAssignedRetryTopic(safeConsumer.assignment()));
                partitionDataManager.resetAllPartitionData();
            } catch (Exception ex) {
                logger.warn("seekToBeginning error. due to ", ex);
            } finally {
                syncLock.unlock();
            }
        } else {
            logger.info("Consume service  hasn't been initialized.");
        }
    }

    @Override
    public void seekToEnd() {
        if (isRunning) {
            syncLock.lock();
            try {
                safeConsumer.seekToEnd(filterAssignedRetryTopic(safeConsumer.assignment()));
                partitionDataManager.resetAllPartitionData();
            } catch (Exception ex) {
                logger.warn("seekToEnd error. due to ", ex);
            } finally {
                syncLock.unlock();
            }
        } else {
            logger.info("Consume service hasn't been initialized.");
        }
    }

    @Override
    public ConsumerRebalanceListener getRebalanceListener() {
        return offsetPersistor;
    }

    @Override
    public void subscribe(Collection<String> topics) {
        syncLock.lock();
        try {
            safeConsumer.subscribe(topics, offsetPersistor);
        } catch (Exception ex) {
            logger.warn("ConsumeService subscribe error. due to ", ex);
        } finally {
            syncLock.unlock();
        }
    }

    @Override
    public void unsubscribe() {
        syncLock.lock();
        try {
            safeConsumer.unsubscribe();
        } catch (Exception ex) {
            logger.warn("cancel topic subscribe error. due to ", ex);
        } finally {
            syncLock.unlock();
        }
    }

    @Override
    public void suspend() {
        syncLock.lock();
        try {
            safeConsumer.pause(safeConsumer.assignment());
        } catch (Exception ex) {
            logger.warn("suspend consumer service error. due to ", ex);
        } finally {
            syncLock.unlock();
        }
    }

    @Override
    public void resume() {
        syncLock.lock();
        try {
            safeConsumer.resume(safeConsumer.assignment());
        } catch (Exception ex) {
            logger.warn("resume consumer service error. due to ", ex);
        } finally {
            syncLock.unlock();
        }
    }

    public void dispatchTaskLater(final AbsConsumeTaskRequest<K> requestTask, final long timeout, final TimeUnit unit) {
        try {
            scheduleExecTaskService.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        dispatchTaskAtOnce(requestTask);
                    } catch (Exception ex1) {
                        logger.warn("dispatchTaskAtOnce() failed, because partition is full. invoke dispatchTaskLater().", ex1);
                        dispatchTaskLater(requestTask, timeout, unit);
                    }
                }
            }, timeout, unit);
        } catch (Throwable t) {
            logger.warn("dispatchTaskLater submit task failed.", t);
            try {
                TimeUnit.MILLISECONDS.sleep(unit.toMillis(timeout));
            } catch (InterruptedException e) {
            }
            dispatchTaskLater(requestTask, timeout, unit);
        }
    }

    //maybe throw RejectedExecutionException
    public Future<ConsumeTaskResponse> dispatchTaskAtOnce(AbsConsumeTaskRequest<K> requestTask) {
        Future<ConsumeTaskResponse> responseFuture = null;
        if (null != requestTask) {
            responseFuture = execTaskService.submit(requestTask);
            requestTask.setTaskResponseFuture(responseFuture);
        }
        return responseFuture;
    }

}
