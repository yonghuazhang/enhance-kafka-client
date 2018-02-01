package org.apache.kafka.clients.enhance.consumer;

import com.sun.corba.se.impl.orbutil.concurrent.Sync;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.ShutdownableThread;
import org.apache.kafka.clients.enhance.Utility;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by steven03.zhang on 2017/12/13.
 */
class KafkaPollMessageService<K> extends ShutdownableThread {
    private final static Logger logger = LoggerFactory.getLogger(KafkaPollMessageService.class);

    private final ConsumerWithAdmin<K> safeConsumer;
    private final AtomicReference<RuntimeException> failed = new AtomicReference<>(null);
    private final PartitionDataManager<K, ExtMessage<K>> partitionDataManager;
    private final ConsumeClientContext<K> clientContext;

    private volatile boolean isRunning;
    private volatile boolean isSuspend;

    public KafkaPollMessageService(String serviceName, ConsumerWithAdmin<K> safeConsumer,
                                   PartitionDataManager<K, ExtMessage<K>> partitionDataManager,
                                   ConsumeClientContext<K> clientContext) {
        super(serviceName);
        this.safeConsumer = safeConsumer;
        this.clientContext = clientContext;
        this.partitionDataManager = partitionDataManager;
    }

    public void setSuspend(boolean isSuspend) {
        this.isSuspend = isSuspend;
    }

    @Override
    public void doWork() {
        while (isRunning) {
            if (!isSuspend) {
                try {
                    ConsumerRecords<K, ExtMessage<K>> records = safeConsumer.poll(clientContext.pollMessageAwaitTimeoutMs());
                    if (!records.isEmpty()) {
                        Set<TopicPartition> needPausedPartitions = partitionDataManager.saveConsumerRecords(records);
                        Set<TopicPartition> hasPausedPartitions = safeConsumer.paused();

                        if (!hasPausedPartitions.isEmpty()) {
                            Set<TopicPartition> needResumePartitions = new HashSet<>();
                            for (TopicPartition tp : hasPausedPartitions) {
                                if (!needPausedPartitions.contains(tp)) {
                                    needResumePartitions.add(tp);
                                }
                            }
                            if (!needResumePartitions.isEmpty()) {
                                safeConsumer.resume(needResumePartitions);
                            }
                        }
                        safeConsumer.pause(needPausedPartitions);

                    }
                } catch (Throwable t) {
                    logger.error("KafkaPollMessageService error. due to ", t);
                }
            } else {
                Utility.sleep(clientContext.pollMessageAwaitTimeoutMs());
            }
        }
    }

    public void startService() {
        if (!isRunning) {
            isRunning = true;
            try {
                super.start();
                dispatchTaskThread.start();
                consumerTimer.schedule(new CommitTask(), 1000L, 5000L);
            } catch (Exception e) {
                logger.warn("start service error.", e);
                shutdown();
                throw new KafkaException("start Service not success.", e);
            }
        }
    }

    @Override
    public void shutdown() {
        if (isRunning) {
            isRunning = false;
            if (isAlive()) {
                super.shutdown();
            }
            if (dispatchTaskThread.isAlive()) {
                dispatchTaskThread.shutdown();
            }
            scheduledExecService.shutdownNow();
            jobQueue.clear();
            execService.shutdownNow();
            consumerTimer.cancel();

            kafkaProducer.close(5, TimeUnit.SECONDS);
            partitionDataManager.storeGroupOffsets();

        }

    }


}
