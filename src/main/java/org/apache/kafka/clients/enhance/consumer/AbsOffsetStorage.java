package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.Utility;
import org.apache.kafka.clients.enhance.exception.ConsumeOffsetException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.kafka.clients.enhance.ExtMessageDef.INVALID_OFFSET_VALUE;
import static org.apache.kafka.clients.enhance.Utility.INVALID_TIMESTAMP;
import static org.apache.kafka.clients.enhance.consumer.ExtResetStrategy.RESET_FROM_TIMESTAMP;

/**
 * abstract GroupOffsetPersistor.
 */
public abstract class AbsOffsetStorage<K> implements ConsumerRebalanceListener {
    protected static final Logger logger = LoggerFactory.getLogger(AbsOffsetStorage.class);

    private static final String LOCAL_OFFSETS_STORE_NAME = "local-offset-store.dat";
    public static final OffsetAndMetadata INVILID_OFFSET_META = new OffsetAndMetadata(INVALID_OFFSET_VALUE);
    protected final Map<TopicPartition, OffsetAndMetadata> commitedOffsetSnapshot = new HashMap<>();
    protected Object lock = new Object();

    protected final LoadOffsetType loadType;
    protected final ConsumerWithAdmin<K> safeConsumer;
    protected final PartitionDataManager<K, ExtMessage<K>> partitionDataManager;
    protected final ConsumeClientContext clientContext;
    private final Timer storeTimer = new Timer();
    private final TimerTask offsetStoreTask = new OffsetStoreTask();

    protected AbsOffsetStorage(ConsumerWithAdmin<K> safeConsumer, PartitionDataManager partitionDataManager,
                               ConsumeClientContext clientContext, LoadOffsetType loadType) {
        this.loadType = loadType;
        this.safeConsumer = safeConsumer;
        this.partitionDataManager = partitionDataManager;
        this.clientContext = clientContext;
        //this.loadFromStorage = load();
    }

    public void start() {
        synchronized (lock) {
            load();
            storeTimer.scheduleAtFixedRate(offsetStoreTask, clientContext.offsetStoreIntervals(),
                    clientContext.offsetStoreIntervals());
        }
    }

    public void shutdown() {
        synchronized (lock) {
            storeAllOffsetMeta();
            commitedOffsetSnapshot.clear();
            storeTimer.cancel();
        }
    }

    class OffsetStoreTask extends TimerTask {

        @Override
        public void run() {
            Map<TopicPartition, OffsetAndMetadata> needCommitOffset = latestNeedAckOffsets();
            if (null != needCommitOffset && !needCommitOffset.isEmpty()) {
                try {
                    storeOffsetMeta(needCommitOffset);
                } catch (ConsumeOffsetException ex) {
                    // exception needn't redo, because storeOffsetMeta will do again in the next timertask
                    logger.warn("storeOffsetMeta error. due to ", ex);
                }
            }
        }

    }

    public abstract boolean load();

    public abstract void storeOffsetMeta(Map<TopicPartition, OffsetAndMetadata> ackOffsetMeta) throws ConsumeOffsetException;

    public void storeAllOffsetMeta() {
        try {
            latestNeedAckOffsets();
            storeOffsetMeta(commitedOffsetSnapshot);
        } catch (Exception ex) {
            logger.warn("storeAllOffsetMeta error. due to ", ex);
        }
    }

    public boolean updateOffset(TopicPartition tp, OffsetAndMetadata newOffsetMeta, boolean increaseOnly) {
        if (null == tp || INVILID_OFFSET_META.equals(newOffsetMeta)) {
            return false;
        }
        long updateOffset = INVALID_OFFSET_VALUE;
        synchronized (lock) {
            OffsetAndMetadata tmpOffset = commitedOffsetSnapshot.get(tp);
            long prevOffset = tmpOffset.offset();
            long newOffset = newOffsetMeta.offset();
            if (increaseOnly) {
                if (newOffset > prevOffset) {
                    updateOffset = newOffset;
                }
            } else {
                if (newOffset != prevOffset) {
                    updateOffset = newOffset;
                }
            }
            if (INVALID_OFFSET_VALUE != updateOffset) {
                commitedOffsetSnapshot.put(tp, new OffsetAndMetadata(updateOffset));
                return true;
            }
        }
        return false;
    }

    public void clearOffset() {
        synchronized (lock) {
            commitedOffsetSnapshot.clear();
        }
    }

    public OffsetAndMetadata removeOffset(TopicPartition tp) {
        OffsetAndMetadata result = null;
        synchronized (lock) {
            if (commitedOffsetSnapshot.containsKey(tp)) {
                result = commitedOffsetSnapshot.remove(tp);
            }
        }
        return (result == null) ? INVILID_OFFSET_META : result;
    }

    //update offsetMeta in commitedOffsetSnapshot and return the offsetMeta which have been changed
    private Map<TopicPartition, OffsetAndMetadata> latestNeedAckOffsets() {
        Map<TopicPartition, OffsetAndMetadata> latestAcks = partitionDataManager.latestAckOffsets();
        Map<TopicPartition, OffsetAndMetadata> needCommitOffset = new HashMap<>(latestAcks.size());
        synchronized (lock) {
            for (TopicPartition tp : latestAcks.keySet()) {
                OffsetAndMetadata newOffset = latestAcks.get(tp);
                if (commitedOffsetSnapshot.containsKey(tp)) {
                    if (updateOffset(tp, newOffset, true)) {
                        needCommitOffset.put(tp, newOffset);
                    }
                } else {
                    commitedOffsetSnapshot.put(tp, newOffset);
                    needCommitOffset.put(tp, newOffset);
                }
            }
        }
        return needCommitOffset;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        //persist all partition offsets

        //update partition Manager status
        partitionDataManager.updateOnPartitionsRevoked(partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        ExtResetStrategy strategy = clientContext.resetStrategy();

        //remove none-assigned partition
        synchronized (lock) {
            if (!commitedOffsetSnapshot.isEmpty()) {
                commitedOffsetSnapshot.keySet().retainAll(partitions);
            }
        }

        //first start
        if (LoadOffsetType.LOAD_FROM_LOCAL_FILE == loadType && RESET_FROM_TIMESTAMP != strategy) {
            synchronized (lock) {
                for (TopicPartition tp : partitions) {
                    if (commitedOffsetSnapshot.containsKey(tp)) {
                        OffsetAndMetadata offsetMeta = commitedOffsetSnapshot.get(tp);
                        if (!offsetMeta.equals(INVALID_OFFSET_VALUE)) {
                            try {
                                safeConsumer.seek(tp, offsetMeta.offset());
                            } catch (Exception ex) {
                                logger.warn("[onPartitionsAssigned] load offsets from storage failed. due to", ex);
                            }
                        }
                    }// new assigned partition will be joined when latestNeedAckOffsets() invoked.
                }
            }
        }

        //if ExtResetStrategy == RESET_FROM_TIMESTAMP, if timestamp is Valid, will seek to the offset according to time.
        if (strategy == RESET_FROM_TIMESTAMP && strategy.isValidTimestamp()) {

            HashMap<TopicPartition, Long> searchByTimestamp = new HashMap<>();
            for (TopicPartition tp : partitions) {
                searchByTimestamp.put(tp, strategy.timestamp());
            }

            try {
                Map<TopicPartition, OffsetAndTimestamp> searchByTimeMap = safeConsumer.offsetsForTimes(searchByTimestamp);
                for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : searchByTimeMap.entrySet()) {
                    try {
                        safeConsumer.seek(entry.getKey(), entry.getValue().offset());
                        synchronized (lock) {
                            commitedOffsetSnapshot.put(entry.getKey(), new OffsetAndMetadata(entry.getValue().offset()));
                        }
                    } catch (Exception ex1) {
                        logger.warn("RESET_FROM_TIMESTAMP seek offset [{}] error. due to ", entry, ex1);
                    }
                }
            } catch (Exception ex2) {
                logger.warn("RESET_FROM_TIMESTAMP reset message position error. due to ", ex2);
            }
            //only execute one time when start,
            RESET_FROM_TIMESTAMP.timestamp(INVALID_TIMESTAMP);
        }

        //update partition Manager status
        partitionDataManager.updateOnPartitionsAssigned(partitions);

    }


    protected String localOffsetFileName() {
        return Utils.join(Arrays.asList(System.getProperty("user.home"), safeConsumer.groupId(),
                safeConsumer.clientId(), Utility.getLocalAddress(), LOCAL_OFFSETS_STORE_NAME), File.separator);
    }

    protected String localOffsetBkFileName() {
        return localOffsetFileName() + ".bk";
    }
}
