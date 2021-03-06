package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.enhance.AbstractExtMessageFilter;
import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.ShutdownableThread;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.kafka.clients.enhance.ExtMessageDef.INVALID_OFFSET_VALUE;

class KafkaPollMessageService<K> extends ShutdownableThread {
	private final static Logger logger = LoggerFactory.getLogger(KafkaPollMessageService.class);

	private final EnhanceConsumer<K> safeConsumer;
	private final PartitionDataManager<K, ExtMessage<K>> partitionDataManager;
	private final ConsumeClientContext<K> clientContext;
	private final ReentrantLock consumeServiceLock;

	private volatile boolean isRunning = false;
	private volatile boolean isSuspend = false;

	public KafkaPollMessageService(String serviceName, EnhanceConsumer<K> safeConsumer,
			PartitionDataManager<K, ExtMessage<K>> partitionDataManager, ConsumeClientContext<K> clientContext,
			ReentrantLock consumeServiceLock) {
		super(serviceName);
		this.safeConsumer = safeConsumer;
		this.clientContext = clientContext;
		this.partitionDataManager = partitionDataManager;
		this.consumeServiceLock = consumeServiceLock;
	}

	public void stopPollingMessage() {
		consumeServiceLock.lock();
		try {
			safeConsumer.pause(safeConsumer.assignment());
			isSuspend = true;
		} catch (Exception ex) {
			logger.warn("KafkaPollMessageService suspend polling message error. due to ", ex);
		} finally {
			consumeServiceLock.unlock();
		}
	}

	public void resumePollingMessage() {
		consumeServiceLock.lock();
		try {
			safeConsumer.resume(safeConsumer.assignment());
			isSuspend = false;
		} catch (Exception ex) {
			logger.warn("KafkaPollMessageService resume polling message error. due to ", ex);
		} finally {
			consumeServiceLock.unlock();
		}
	}

	@Override
	public void doWork() {
		while (isRunning) {
			try {
				if (!isSuspend && consumeServiceLock
						.tryLock(clientContext.pollMessageAwaitTimeoutMs(), TimeUnit.MILLISECONDS)) {
					try {
						ConsumerRecords<K, ExtMessage<K>> records = safeConsumer
								.poll(clientContext.pollMessageAwaitTimeoutMs());
						logger.debug("KafkaPollMessageService retrieve no messages [{}] and records count {}.",
								records.isEmpty(), records.count());
						//filter messages
						Map<TopicPartition, Long> highWaterMarks = new HashMap<>();
						ConsumerRecords<K, ExtMessage<K>> filterMessages = filterMessage(records,
								clientContext.messageFilter(), highWaterMarks);
						Set<TopicPartition> needPausePartitions = partitionDataManager
								.saveConsumerRecords(filterMessages, highWaterMarks);

						Set<TopicPartition> pausedPartitions = safeConsumer.paused();
						if (!pausedPartitions.isEmpty()) {
							Set<TopicPartition> needResumePartitions = new HashSet<>();
							for (TopicPartition tp : pausedPartitions) {
								if (!needPausePartitions.contains(tp)) {
									needResumePartitions.add(tp);
								}
							}
							if (!needResumePartitions.isEmpty()) {
								safeConsumer.resume(needResumePartitions);
							}
						}
						if (null != needPausePartitions && !needPausePartitions.isEmpty()) {
							safeConsumer.pause(needPausePartitions);
						}
					} catch (Throwable t) {
						logger.warn("KafkaPollMessageService error. due to ", t);
					} finally {
						consumeServiceLock.unlock();
					}
				} else {
					TimeUnit.MILLISECONDS.sleep(clientContext.pollMessageAwaitTimeoutMs());
				}
			} catch (InterruptedException e) {
				logger.warn("KafkaPollMessageService is interrupted.");
				Thread.currentThread().interrupt();
			}
		}
	}

	private ConsumerRecords<K, ExtMessage<K>> filterMessage(ConsumerRecords<K, ExtMessage<K>> records,
			AbstractExtMessageFilter<K> filter, Map<TopicPartition, Long> highWaterMarks) {
		if (null == records || records.isEmpty())
			return records;
		if (filter.isPermitAll()) {
			return records;
		}

		Map<TopicPartition, List<ConsumerRecord<K, ExtMessage<K>>>> filterRecords = new HashMap<>(records.count());
		Set<TopicPartition> tps = records.partitions();
		for (TopicPartition tp : tps) {
			ArrayList<ConsumerRecord<K, ExtMessage<K>>> filterRecordsByPartition = new ArrayList<>();
			List<ConsumerRecord<K, ExtMessage<K>>> partitionRecords = records.records(tp);
			long maxOffsetInPartition = INVALID_OFFSET_VALUE;
			for (ConsumerRecord<K, ExtMessage<K>> partitionRecord : partitionRecords) {
				maxOffsetInPartition = Math.max(maxOffsetInPartition,partitionRecord.offset());
				if (filter.canDeliveryMessage(partitionRecord.value(), partitionRecord.headers())) {
					filterRecordsByPartition.add(partitionRecord);
				}
			}
			highWaterMarks.put(tp, maxOffsetInPartition);
			filterRecords.put(tp, filterRecordsByPartition);
		}

		return new ConsumerRecords<>(filterRecords);
	}

	@Override
	public void start() {
		if (!isRunning) {
			synchronized (this) {
				if (!isRunning) {
					isRunning = true;
					try {
						super.start();
					} catch (Exception e) {
						logger.warn("start KafkaPollMessageService service error. due to ", e);
						shutdown();
						throw new KafkaException("start KafkaPollMessageService failed.");
					}
				}
			}
		} else {
			logger.info("KafkaPollMessageService has been started.");
		}
	}

	@Override
	public void shutdown() {
		if (isRunning) {
			synchronized (this) {
				if (isRunning) {
					isRunning = false;
					if (isAlive()) {
						super.shutdown();
					}
				}
			}
		} else {
			logger.info("KafkaPollMessageService has been shutdown.");
		}
	}

}
