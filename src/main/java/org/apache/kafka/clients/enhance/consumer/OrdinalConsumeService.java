package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.ShutdownableThread;
import org.apache.kafka.clients.enhance.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class OrdinalConsumeService<K> extends AbstractConsumeService<K> {
	private final ConcurrentHashMap<TopicPartition, OrdinalConsumeTaskRequest<K>> ordinalTasks = new ConcurrentHashMap<>();

	public OrdinalConsumeService(EnhanceConsumer<K> safeConsumer, KafkaProducer<K, ExtMessage<K>> innerSender,
			ConsumeClientContext<K> clientContext) {
		super(safeConsumer, innerSender, clientContext);
		this.dispatchService = new OrdinalDispatchMessageService("Ordinal-dispatch-message-service-thread");
	}

	public ConcurrentHashMap<TopicPartition, OrdinalConsumeTaskRequest<K>> getConsumeTasks() {
		return ordinalTasks;
	}

	public class OrdinalDispatchMessageService extends ShutdownableThread {

		public OrdinalDispatchMessageService(String name) {
			super(name);
		}

		@Override
		public void doWork() {
			while (isRunning) {
				Set<TopicPartition> assignedTopicPartitions = safeConsumer.assignment();
				if (null != assignedTopicPartitions && !assignedTopicPartitions.isEmpty()) {
					for (TopicPartition topicPartition : assignedTopicPartitions) {
						OrdinalConsumeTaskRequest<K> oldTask = ordinalTasks.get(topicPartition);
						if (null == oldTask) {
							List<ConsumerRecord<K, ExtMessage<K>>> records = partitionDataManager
									.retrieveTaskRecords(topicPartition, clientContext.consumeBatchSize());
							if (!records.isEmpty()) {
								List<ExtMessage<K>> messages = new ArrayList<>(records.size());
								for (ConsumerRecord<K, ExtMessage<K>> record : records) {
									messages.add(record.value());
								}
								OrdinalConsumeTaskRequest<K> requestTask = new OrdinalConsumeTaskRequest<>(
										OrdinalConsumeService.this, partitionDataManager, messages, topicPartition,
										clientContext);
								logger.debug("[OrdinalConsumeService] dispatch consuming task at once. messages = "
										+ messages);

								submitConsumeRequest(requestTask);
								ordinalTasks.put(topicPartition, requestTask);
							}
						} else {
							logger.debug("[OrdinalConsumeService] task was executing now." + oldTask);
						}
					}

				} else {
					Utility.sleep(clientContext.pollMessageAwaitTimeoutMs());
				}
				cleanNonAssignedPartitionTask(assignedTopicPartitions);
			}
		}

		private void cleanNonAssignedPartitionTask(Set<TopicPartition> assignedPartition) {
			if (null == assignedPartition || assignedPartition.isEmpty()) {
				for (TopicPartition tp : ordinalTasks.keySet()) {
					closeNonAssignedPartitionTask(tp);
				}
				ordinalTasks.clear();
			} else {
				Set<TopicPartition> taskPartitions = ordinalTasks.keySet();
				if (!assignedPartition.containsAll(taskPartitions)) {
					for (TopicPartition tp : taskPartitions) {
						if (!assignedPartition.contains(tp)) {
							closeNonAssignedPartitionTask(tp);
						}
					}
				}
			}

		}

		private void closeNonAssignedPartitionTask(TopicPartition tp) {
			OrdinalConsumeTaskRequest<K> request = ordinalTasks.get(tp);
			if (null != request && null != request.taskResponseFuture) {
				try {
					request.setShutdownTask(true);
					request.taskResponseFuture.cancel(true);
				} finally {
					ordinalTasks.remove(tp);
				}
			}
		}
	}

}
