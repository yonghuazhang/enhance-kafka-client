package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.enhance.exception.PartitionDataFullException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.kafka.clients.enhance.ExtMessageDef.INVALID_OFFSET_VALUE;

public class PartitionDataManager<K, V> {
    private final static Logger logger = LoggerFactory.getLogger(PartitionDataManager.class);

    final static int MAX_OFFSET_INTERVALS = 2000;
    final static int MAX_SIZE_SLIDING_WINDOWS = 1024;
    final int MAX_CONSUME_TIME_INTERVALS_IN_MS = 5 * 60 * 1000;
    private final ConcurrentHashMap<TopicPartition, PartitionData<K, V>> patitionDatas = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TopicPartition, List<ConsumerRecord<K, V>>> saveFailedRecords = new ConcurrentHashMap<>();

    public void updateOnPartitionsRevoked(final Collection<TopicPartition> partitions) {
        logger.info("[PartitionDataManager] called by client rebalance onPartitionsRevoked ...");
        saveFailedRecords.clear();
        if (null != partitions && !partitions.isEmpty()) {
            for (TopicPartition tp : patitionDatas.keySet()) {
                PartitionData<?, ?> pd = patitionDatas.get(tp);
                if (null != pd) {
                    pd.setValid(false);
                } else {
                    partitions.remove(tp);
                    logger.info("[PartitionDataManager] partition data not exists for tp [{}].", tp);
                }
            }
        }
    }

    public boolean updateOnPartitionsAssigned(final Collection<TopicPartition> partitions) {
        logger.info("[PartitionDataManager] called by client rebalance OnPartitionsAssigned ...");
        saveFailedRecords.clear();
        if (null != partitions && !partitions.isEmpty()) {
            Set<TopicPartition> keys = patitionDatas.keySet();
            keys.removeAll(partitions);
            for (TopicPartition tp : keys) {
                PartitionData<?, ?> pd = patitionDatas.remove(tp);
                if (null != pd) {
                    pd.clear();
                }
            }

            for (TopicPartition tp : partitions) {
                if (patitionDatas.containsKey(tp)) {
                    PartitionData<K, V> pd = patitionDatas.get(tp);
                    pd.clear();
                    pd.setValid(true);
                } else {
                    patitionDatas.putIfAbsent(tp, new PartitionData<K, V>(tp));
                }
            }
        }

        return true;
    }

    public Set<TopicPartition> getAssignedPartition() {
        return patitionDatas.keySet();
    }

    public Map<TopicPartition, OffsetAndMetadata> latestAckOffsets() {
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = new HashMap<>();
        for (TopicPartition tp : patitionDatas.keySet()) {
            PartitionData<K, V> pd = patitionDatas.get(tp);
            if (null != pd && pd.getLastAckOffset() != INVALID_OFFSET_VALUE) {
                long ackOffset = pd.getLastAckOffset() + 1;
                logger.debug("[PartitionDataManager-latestAckOffsets] retrieve lastestAck offset, TopicPartition = [{}] | offet = [{}].", tp, ackOffset);
                commitOffsets.put(tp, new OffsetAndMetadata(ackOffset));
            }
        }
        return commitOffsets;
    }

    public Set<TopicPartition> saveConsumerRecords(ConsumerRecords<K, V> records) throws InterruptedException {
        //save current messages from latest polled message.
        if (null != records && !records.isEmpty()) {
            for (TopicPartition tp : records.partitions()) {
                if (!patitionDatas.containsKey(tp)) {
                    patitionDatas.putIfAbsent(tp, new PartitionData<K, V>(tp));
                }
                PartitionData<K, V> partitionData = patitionDatas.get(tp);
                List<ConsumerRecord<K, V>> recordsByPartition = records.records(tp);
                savePartitionData(tp, partitionData, recordsByPartition);
            }
        }
        //save last rest records.
        saveLastRestPartitionData();
        return Collections.unmodifiableSet(saveFailedRecords.keySet());
    }

    private void saveLastRestPartitionData() throws InterruptedException {
        if (saveFailedRecords.isEmpty()) return;
        for (TopicPartition tp : saveFailedRecords.keySet()) {
            List<ConsumerRecord<K, V>> lastRestRecords = saveFailedRecords.remove(tp);

            if (!patitionDatas.containsKey(tp)) {
                logger.warn("[PartitionDataManager] saveLastRestPartitionData not contains topicPartition [{}].", tp);
                patitionDatas.putIfAbsent(tp, new PartitionData<K, V>(tp));
            }
            PartitionData<K, V> partitionData = patitionDatas.get(tp);
            savePartitionData(tp, partitionData, lastRestRecords);
        }
    }

    private boolean savePartitionData(final TopicPartition tp, final PartitionData<K, V> partitionData, final List<ConsumerRecord<K, V>> recordsByPartition) throws InterruptedException {
        if (null == recordsByPartition || recordsByPartition.isEmpty()) return true;
        try {
            int saveNum = partitionData.putRecords(recordsByPartition);
            logger.debug("[PartitionDataManager] the number of records is [{}], save successful records is [{}].", recordsByPartition.size(), saveNum);
            return true;
        } catch (InterruptedException e) {
            logger.warn("[PartitionDataManager] save data is interrupted.", e);
            throw e;
        } catch (PartitionDataFullException e) {
            logger.info("[PartitionDataManager] Partition[{}] data is full. the partition will be paused", tp);
            if (e.getFromIdx() >= 0L) {
                List<ConsumerRecord<K, V>> notSavedRecords = recordsByPartition.subList(e.getFromIdx(), recordsByPartition.size() - 1);
                if (saveFailedRecords.containsKey(tp)) {
                    saveFailedRecords.get(tp).addAll(notSavedRecords);
                } else {
                    List<ConsumerRecord<K, V>> oldRecords = saveFailedRecords.putIfAbsent(tp, notSavedRecords);
                    if (null != oldRecords && !oldRecords.isEmpty()) {
                        saveFailedRecords.get(tp).addAll(oldRecords);
                    }
                }
                logger.debug("[PartitionDataManager] the number of the records which need be saved is [{}]. not saved record's num is [{}].", recordsByPartition.size(), notSavedRecords.size());
            }
        }
        return false;
    }

    public List<ConsumerRecord<K, V>> retrieveTaskRecords(TopicPartition tp, int batchSize) {
        PartitionData<K, V> pd = patitionDatas.get(tp);
        if (null != pd && pd.isValid()) {
            try {
                return pd.takeRecords(batchSize);
            } catch (InterruptedException e) {
                logger.warn("[PartitionDataManager] retrieveTaskRecords is interrupted.", e);
                Thread.currentThread().interrupt();
            }
        }
        return Collections.emptyList();
    }

    public void commitOffsets(TopicPartition tp, List<Long> offsets) {
        if (null == tp || offsets.isEmpty()) return;
        PartitionData<K, V> pd = patitionDatas.get(tp);
        if (null != pd) {
            try {
                pd.removeRecord(offsets);
            } catch (InterruptedException e) {
                logger.warn("[PartitionDataManager] commitOffsets is interrupted.", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    public void commitOffset(TopicPartition tp, long offset) {
        PartitionData<K, V> pd = patitionDatas.get(tp);
        if (null != pd && pd.isValid()) {
            try {
                pd.removeRecord(offset);
            } catch (InterruptedException e) {
                logger.warn("[PartitionDataManager] commitOffsets is interrupted.", e);
            }
        }
    }

    public void resetPartitionData(TopicPartition tp) {
        PartitionData<K, V> pd = patitionDatas.get(tp);
        if (null != pd) {
            pd.resetPartition();
        }
    }

    public void resetAllPartitionData() {
        for (PartitionData<K, V> pd : patitionDatas.values()) {
            if (null != pd) {
                pd.resetPartition();
            }
        }
    }
}
