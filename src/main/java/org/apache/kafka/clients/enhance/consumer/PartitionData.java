package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.enhance.exception.PartitionDataFullException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.kafka.clients.enhance.ExtMessageDef.INVALID_OFFSET_VALUE;

public class PartitionData<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(PartitionData.class);
    static final long SLIDING_WINDOWS_FULL_AWAIT_TIMOUT_MS = 200L;

    private final TopicPartition tp;
    private final TreeMap<Long, ConsumerRecord<K, V>> slidingWindow;
    //iterator red-black tree and retrieve the size is too slow.
    private final AtomicInteger slidingWindowSize = new AtomicInteger(0);
    private final AtomicLong takeCursor = new AtomicLong(INVALID_OFFSET_VALUE);
    private final AtomicLong accTakeMessageNum = new AtomicLong(0L);
    private final AtomicLong pMaxOffset = new AtomicLong(INVALID_OFFSET_VALUE);
    private final AtomicLong lastAckOffset = new AtomicLong(INVALID_OFFSET_VALUE);

    private final Time kafkaTime = Time.SYSTEM;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock wLock = rwLock.writeLock();
    private final ReentrantReadWriteLock.ReadLock rLock = rwLock.readLock();
    private final Condition winFullLock = wLock.newCondition();

    private volatile boolean isValid = true;
    private volatile long lastPutMessageTimestamp;
    private volatile long lastConsumedTimestamp;


    public long getLastPutMessageTimestamp() {
        return lastPutMessageTimestamp;
    }

    public long getLastConsumedTimestamp() {
        return lastConsumedTimestamp;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    public PartitionData(TopicPartition tp) {
        this.tp = tp;
        this.slidingWindow = new TreeMap<>();
    }

    private boolean isWinFull(int estimateNum) {
        return (slidingWindowSize.get() + estimateNum) > PartitionDataManager.MAX_SIZE_SLIDING_WINDOWS;
    }

    public int putRecord(ConsumerRecord<K, V> record) throws InterruptedException, PartitionDataFullException {
        int recCnt = 0;
        if (isPartitionRecord(record)) {
            wLock.lockInterruptibly();
            try {
                if (isWinFull(1)) {
                    boolean awaitOk = winFullLock.await(SLIDING_WINDOWS_FULL_AWAIT_TIMOUT_MS, TimeUnit.MILLISECONDS);
                    if (!awaitOk) {
                        throw new PartitionDataFullException("put message await lock timeout.");
                    }
                }
                ConsumerRecord<K, V> oldRec = slidingWindow.put(record.offset(), record);
                if (null == oldRec) {
                    recCnt++;
                    slidingWindowSize.getAndIncrement();
                }
            } finally {
                lastPutMessageTimestamp = kafkaTime.milliseconds();
                pMaxOffset.set(slidingWindow.lastKey().longValue());
                wLock.unlock();
            }
        }
        return recCnt;
    }

    public int putRecords(List<ConsumerRecord<K, V>> records) throws InterruptedException, PartitionDataFullException {
        int recCnt = 0;
        if (null == records || records.isEmpty()) return recCnt;
        wLock.lockInterruptibly();
        try {
            for (int fromIdx = 0; fromIdx < records.size(); fromIdx++) {
                ConsumerRecord<K, V> record = records.get(fromIdx);
                if (isWinFull(1)) {
                    boolean awaitOk = winFullLock.await(SLIDING_WINDOWS_FULL_AWAIT_TIMOUT_MS, TimeUnit.MILLISECONDS);
                    if (!awaitOk) {
                        throw new PartitionDataFullException("put message await lock timeout.", fromIdx);
                    }
                }
                if (record.offset() > pMaxOffset.get()) {
                    ConsumerRecord<K, V> oldRec = slidingWindow.put(record.offset(), record);
                    if (null == oldRec) {
                        recCnt++;
                    }
                }
            }
            slidingWindowSize.addAndGet(recCnt);
            return recCnt;
        } finally {
            lastPutMessageTimestamp = kafkaTime.milliseconds();
            pMaxOffset.set(slidingWindow.lastKey().longValue());
            /*logger.debug("[putRecords] tp = " + tp + "\t takeCursor=" + takeCursor.get() + "\t pMaxOffset = " + pMaxOffset.get()
                    + "\t lastAckOffset=" + lastAckOffset.get() + "\t winSize=" + slidingWindowSize.get());*/
            wLock.unlock();
        }
    }

    public int removeRecord(long offset) throws InterruptedException {
        wLock.lockInterruptibly();
        int removeCnt = 0;
        try {
            ConsumerRecord<K, V> oldRec = slidingWindow.remove(offset);
            if (null != oldRec) {
                removeCnt++;
            }
            slidingWindowSize.addAndGet(-1 * removeCnt);
            return removeCnt;
        } finally {
            if (removeCnt > 0 && slidingWindowSize.get() < PartitionDataManager.MAX_SIZE_SLIDING_WINDOWS) {
                winFullLock.signalAll();
            }
            if (slidingWindow.isEmpty()) {
                long partitionMaxPutOffset = pMaxOffset.get();
                long tmpMaxOffset = (partitionMaxPutOffset > offset) ? partitionMaxPutOffset : offset;
                if (tmpMaxOffset > lastAckOffset.get()) {
                    lastAckOffset.set(tmpMaxOffset);
                }
            } else {
                lastAckOffset.set(slidingWindow.firstKey().longValue() - 1L);
            }
            lastConsumedTimestamp = kafkaTime.milliseconds();
            wLock.unlock();
        }
    }

    public int removeRecord(List<Long> offsets) throws InterruptedException {
        if (null == offsets || offsets.isEmpty()) return 0;
        wLock.lockInterruptibly();
        int removeCnt = 0;
        try {
            for (Long offset : offsets) {
                ConsumerRecord<K, V> oldRec = slidingWindow.remove(offset);
                if (null != oldRec) {
                    removeCnt++;
                }
            }
            slidingWindowSize.addAndGet(-1 * removeCnt);
            return removeCnt;
        } finally {
            if (removeCnt > 0 && slidingWindowSize.get() < PartitionDataManager.MAX_SIZE_SLIDING_WINDOWS) {
                winFullLock.signalAll();
            }
            if (slidingWindow.isEmpty()) {
                long tmpMaxOffset = maxOffset(offsets);
                long partitionMaxPutOffset = pMaxOffset.get();
                tmpMaxOffset = (partitionMaxPutOffset > tmpMaxOffset) ? partitionMaxPutOffset : tmpMaxOffset;
                if (tmpMaxOffset > lastAckOffset.get()) {
                    lastAckOffset.set(tmpMaxOffset);
                }
            } else {
                lastAckOffset.set(slidingWindow.firstKey().longValue() - 1L);
            }
            logger.debug("[removeRecord] tp = " + tp + "\t takeCursor=" + takeCursor.get() + "\t pMaxOffset = " + pMaxOffset.get()
                    + "\t lastAckOffset=" + lastAckOffset.get() + "\t winSize=" + slidingWindowSize.get());

            lastConsumedTimestamp = kafkaTime.milliseconds();
            wLock.unlock();
        }
    }

    private long maxOffset(List<Long> offsets) {
        if (null == offsets || offsets.isEmpty()) return -1;
        if (offsets.size() == 1) return offsets.get(0);
        long maxOffset = offsets.get(0);
        for (Long offset : offsets) {
            maxOffset = Math.max(offset, maxOffset);
        }
        return maxOffset;
    }

    public List<ConsumerRecord<K, V>> takeRecords(final int batchSize) throws InterruptedException {
        if (isValid && hasUndispatchedMessage()) {
            List<ConsumerRecord<K, V>> results = new ArrayList<>(batchSize);
            if (INVALID_OFFSET_VALUE == takeCursor.get()) {
                takeCursor.set(getMinOffset());
                logger.debug("############ reset takeCursor value " + takeCursor.get());
            }
            int num = 0;
            long cur = takeCursor.get();
            for (; cur <= pMaxOffset.get() && num < batchSize; num++, cur++) {
                rLock.lockInterruptibly();
                try {
                    if (slidingWindow.containsKey(cur)) {
                        results.add(slidingWindow.get(cur));
                        logger.debug("############ offset exists " + cur);
                    } else {
                        logger.debug("############ offset not exists" + cur);
                    }
                } finally {
                    rLock.unlock();
                }
            }

            takeCursor.set(cur);
            logger.debug("[takeRecords] tp = " + tp + "\t takeCursor=" +
                    takeCursor.get() + "\t pMaxOffset = " + pMaxOffset.get() + "\t lastAckOffset=" + lastAckOffset.get());
            accTakeMessageNum.addAndGet(num);
            return results;
        }
        return Collections.emptyList();
    }

    public long getTotalTakeMessageNum() {
        return accTakeMessageNum.get();
    }

    public long getMinOffset() throws InterruptedException {
        rLock.lockInterruptibly();
        try {
            if (slidingWindow.isEmpty()) return INVALID_OFFSET_VALUE;
            return slidingWindow.firstKey().longValue();
        } finally {
            rLock.unlock();
        }
    }

    public long getMaxOffset() {
        return pMaxOffset.get();
    }

    public long getOffsetSpan() throws InterruptedException {
        rLock.lockInterruptibly();
        try {
            if (slidingWindow.isEmpty()) return 0;
            return slidingWindow.lastKey().longValue() - slidingWindow.firstKey().longValue();
        } finally {
            rLock.unlock();
        }
    }

    public long getLastAckOffset() {
        return lastAckOffset.get();
    }

    public int getWinSize() {
        return slidingWindowSize.get();
    }

    private boolean hasUndispatchedMessage() throws InterruptedException {
        return (slidingWindowSize.get() > 0) && takeCursor.get() <= pMaxOffset.get();
    }

    private boolean isPartitionRecord(ConsumerRecord<K, V> record) {
        return record.topic().equals(tp.topic()) && record.partition() == tp.partition();
    }

    public void clear() {
        try {
            wLock.lockInterruptibly();
            try {
                isValid = false;
                slidingWindow.clear();
                slidingWindowSize.set(0);
                accTakeMessageNum.set(0L);

                takeCursor.set(INVALID_OFFSET_VALUE);
                pMaxOffset.set(INVALID_OFFSET_VALUE);
                lastAckOffset.set(INVALID_OFFSET_VALUE);
            } finally {
                wLock.unlock();
            }
        } catch (Exception ex) {
            logger.warn("clear is interrupted.");
        }

    }

    public void resetPartition() {
        clear();
        isValid = true;
    }
}
