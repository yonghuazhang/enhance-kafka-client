package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.enhance.Utility;
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
	private final AtomicLong highWaterMarkInWindow = new AtomicLong(INVALID_OFFSET_VALUE);
	private final AtomicLong pullRecordHighWaterMark = new AtomicLong(INVALID_OFFSET_VALUE);
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

	public int putRecord(ConsumerRecord<K, V> record, final long pullHighWaterMark)
			throws InterruptedException, PartitionDataFullException {
		int recCnt = 0;
		if (null == record)
			return recCnt;
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
			highWaterMarkInWindow.set(slidingWindow.lastKey().longValue());
			long maxOffset = Utility.max(highWaterMarkInWindow.get(), pullRecordHighWaterMark.get(), pullHighWaterMark);
			pullRecordHighWaterMark.set(maxOffset);
			lastPutMessageTimestamp = kafkaTime.milliseconds();
			wLock.unlock();
		}
		return recCnt;
	}

	public int putRecords(List<ConsumerRecord<K, V>> records, final long pullHighWaterMark)
			throws InterruptedException, PartitionDataFullException {
		int recCnt = 0;
		if (null == records || records.isEmpty())
			return recCnt;
		wLock.lockInterruptibly();
		try {
			for (int fromIdx = 0; fromIdx < records.size(); fromIdx++) {
				ConsumerRecord<K, V> record = records.get(fromIdx);
				if (isWinFull(1)) {
					boolean waitLockOk = winFullLock.await(SLIDING_WINDOWS_FULL_AWAIT_TIMOUT_MS, TimeUnit.MILLISECONDS);
					if (!waitLockOk) {
						logger.debug(
								"[PartitionData-putRecords] sliding window is full and wait lock timeout, current record offset is [{}].",
								record.offset());
						throw new PartitionDataFullException("put message await lock timeout.", fromIdx);
					}
				}
				if (record.offset() > highWaterMarkInWindow.get()) {
					ConsumerRecord<K, V> oldRec = slidingWindow.put(record.offset(), record);
					if (null == oldRec) {
						recCnt++;
					}
				}
			}
			slidingWindowSize.addAndGet(recCnt);
			return recCnt;
		} finally {
			long maxOffsetInWindow = slidingWindow.lastKey().longValue();
			if (highWaterMarkInWindow.get() < maxOffsetInWindow) {
				highWaterMarkInWindow.set(maxOffsetInWindow);
			}
			pullRecordHighWaterMark
					.set(Utility.max(highWaterMarkInWindow.get(), pullRecordHighWaterMark.get(), pullHighWaterMark));
			/*logger.debug("[putRecords] tp = " + tp + "\t takeCursor=" + takeCursor.get() + "\t highWaterMarkInWindow = " + highWaterMarkInWindow.get()
					+ "\t lastAckOffset=" + lastAckOffset.get() + "\t winSize=" + slidingWindowSize.get());*/
			lastPutMessageTimestamp = kafkaTime.milliseconds();
			wLock.unlock();
		}
	}

	public int removeRecord(long offset) {
		wLock.lock();
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
				long partitionMaxPutOffset = pullRecordHighWaterMark.get();
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

	public int removeRecord(List<Long> offsets) {
		if (null == offsets || offsets.isEmpty())
			return 0;
		wLock.lock();
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
				long partitionMaxPutOffset = pullRecordHighWaterMark.get();
				tmpMaxOffset = (partitionMaxPutOffset > tmpMaxOffset) ? partitionMaxPutOffset : tmpMaxOffset;
				if (tmpMaxOffset > lastAckOffset.get()) {
					lastAckOffset.set(tmpMaxOffset);
				} else {
					logger.debug("local retry message. message offsets = [{}]", offsets);
				}
			} else {
				lastAckOffset.set(slidingWindow.firstKey().longValue() - 1L);
			}
			logger.debug(
					"[removeRecord] tp = " + tp + "\t takeCursor=" + takeCursor.get() + "\t highWaterMarkInWindow = "
							+ highWaterMarkInWindow.get() + "\t lastAckOffset=" + lastAckOffset.get() + "\t winSize="
							+ slidingWindowSize.get());

			lastConsumedTimestamp = kafkaTime.milliseconds();
			wLock.unlock();
		}
	}

	private long maxOffset(List<Long> offsets) {
		if (null == offsets || offsets.isEmpty())
			return -1;
		if (offsets.size() == 1)
			return offsets.get(0);
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
				logger.debug("[PartitionData-takeRecords] ===> reset takeCursor value " + takeCursor.get());
			}
			int num = 0;
			long cur = takeCursor.get();
			for (; cur <= highWaterMarkInWindow.get() && num < batchSize; num++, cur++) {
				rLock.lockInterruptibly();
				try {
					if (slidingWindow.containsKey(cur)) {
						results.add(slidingWindow.get(cur));
						logger.debug("[PartitionData-takeRecords] ===> offset [{}] exists in window.", cur);
					} else {
						logger.debug("[PartitionData-takeRecords] ===> offset [{}] not exists in window.", cur);
					}
				} finally {
					rLock.unlock();
				}
			}

			takeCursor.set(cur);
			logger.debug("[PartitionData-takeRecords] tp = " + tp + "\t takeCursor=" + takeCursor.get()
					+ "\t highWaterMarkInWindow = " + highWaterMarkInWindow.get() + "\t lastAckOffset=" + lastAckOffset
					.get());
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
			if (slidingWindow.isEmpty())
				return INVALID_OFFSET_VALUE;
			return slidingWindow.firstKey().longValue();
		} finally {
			rLock.unlock();
		}
	}

	public long highWaterMarkOffset() {
		return highWaterMarkInWindow.get();
	}

	public long getOffsetSpan() throws InterruptedException {
		rLock.lockInterruptibly();
		try {
			if (slidingWindow.isEmpty())
				return 0;
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
		return (slidingWindowSize.get() > 0) && takeCursor.get() <= highWaterMarkInWindow.get();
	}

	private boolean isPartitionRecord(ConsumerRecord<K, V> record) {
		return record.topic().equals(tp.topic()) && record.partition() == tp.partition();
	}

	public void clear() {
		wLock.lock();
		try {
			isValid = false;
			slidingWindow.clear();
			slidingWindowSize.set(0);
			accTakeMessageNum.set(0L);

			takeCursor.set(INVALID_OFFSET_VALUE);
			highWaterMarkInWindow.set(INVALID_OFFSET_VALUE);
			lastAckOffset.set(INVALID_OFFSET_VALUE);
		} finally {
			wLock.unlock();
		}
	}

	public void resetPartition() {
		wLock.lock();
		try {
			clear();
			isValid = true;
		} finally {
			wLock.unlock();
		}
	}
}
