package org.apache.kafka.clients.enhance.producer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaEnhanceProducer<K> implements ProduceOperator<K> {
	private static final Logger logger = LoggerFactory.getLogger(KafkaEnhanceProducer.class);

	private final ProducerClientContext<K> clientContext = new ProducerClientContext<>();
	private KafkaProducerProxy<K> innerProducer;

	private Object lock = new Object();
	private volatile boolean isRunning;
	private volatile boolean isTransaction;

	public KafkaEnhanceProducer(Map<String, Object> configs) {
		this(configs, null);
	}

	public KafkaEnhanceProducer(Map<String, Object> configs, Class<K> cls) {
		clientContext.producerConfig(configs);
		if (null != cls) {
			clientContext.keySerializer(Serdes.serdeFrom(cls).serializer());
		}
	}

	public KafkaEnhanceProducer(Properties properties) {
		this(properties, null);
	}

	public KafkaEnhanceProducer(Properties properties, Class<K> cls) {
		clientContext.producerConfig(properties);
		if (null != cls) {
			clientContext.keySerializer(Serdes.serdeFrom(cls).serializer());
		}
	}

	public ProducerClientContext<K> producerSetting() {
		return clientContext;
	}

	@Override
	public void flush() {
		if (isRunning && null != innerProducer) {
			innerProducer.flush();
		}
	}

	@Override
	public List<PartitionInfo> partitionsForTopic(String topic) {
		if (isRunning && null != innerProducer) {
			return innerProducer.partitionsFor(topic);
		}
		return Collections.emptyList();
	}

	@Override
	public void beginTransaction() {
		if (isRunning && isTransaction && null != innerProducer) {
			innerProducer.beginTransaction();
		}
	}

	@Override
	public void commitTransaction() {
		if (isRunning && isTransaction && null != innerProducer) {
			innerProducer.commitTransaction();
		}
	}

	@Override
	public void abortTransaction() {
		if (isRunning && isTransaction && null != innerProducer) {
			innerProducer.abortTransaction();
		}
	}

	@Override
	public void sendGroupOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String groupId) {
		if (isRunning && null != innerProducer) {
			innerProducer.sendOffsetsToTransaction(offsets, groupId);
		}
	}

	@Override
	public Future<RecordMetadata> sendMessage(ExtMessage<K> message) {
		if (isRunning && null != innerProducer) {
			return innerProducer.send(new ProducerRecord<>(message.getTopic(), message.getMsgKey(), message));
		} else {
			throw new KafkaException("KafkaEnhanceProducer service hasn't been started.");
		}
	}

	@Override
	public Future<RecordMetadata> sendMessage(ExtMessage<K> message, Callback callback) {
		if (isRunning && null != innerProducer) {
			return innerProducer.send(new ProducerRecord<>(message.getTopic(), message.getMsgKey(), message), callback);
		} else {
			throw new KafkaException("KafkaEnhanceProducer service hasn't been started.");
		}
	}

	@Override
	public void addSendMessageHook(SendMessageHook<K> sendHook) {
		synchronized (lock) {
			clientContext.addSendMessageHook(sendHook);
			if (isRunning && null != innerProducer) {
				innerProducer.addProducerMessageHooks(clientContext.getSendMessageHooks());
			}
		}
	}

	@Override
	public String clientId() {
		return clientContext.clientId();
	}

	@Override
	public void start() {
		synchronized (lock) {
			if (!isRunning) {
				try {
					this.innerProducer = new KafkaProducerProxy<>(clientContext.getProducerConfig());
					if (clientContext.isTransactionProducer()) {
						this.innerProducer.initTransactions();
						this.isTransaction = true;
					}
					if (!clientContext.getSendMessageHooks().isEmpty()) {
						this.innerProducer.addProducerMessageHooks(clientContext.getSendMessageHooks());
					}
					isRunning = true;
				} catch (Exception ex) {
					logger.warn("KafkaEnhanceProducer service fail to start. due to ", ex);
					shutdownNow();
					isRunning = false;
					throw new KafkaException("start KafkaEnhanceProducer failed.");
				}
			} else {
				logger.info("KafkaEnhanceProducer service has been started.");
			}
		}
	}

	@Override
	public void shutdownNow() {
		shutdown(0, TimeUnit.MILLISECONDS);
	}

	@Override
	public void shutdown(long timeout, TimeUnit unit) {
		synchronized (lock) {
			if (isRunning) {
				if (null != this.innerProducer) {
					try {
						this.innerProducer.close(timeout, unit);
						logger.info("KafkaEnhanceProducer has been closed. clientId = [{}].", clientId());
					} catch (Exception e) {
						logger.warn("KafkaEnhanceProducer throw exception when closed. due to ", e);
					}
					this.innerProducer = null;
				}
			} else {
				logger.info("KafkaEnhanceProducer service has been shutdown.");
			}
		}
	}

	@Override
	public void suspend() {
		//since hard to process transaction message, don't implement it.
	}

	@Override
	public void resume() {
		//since hard to process transaction message, don't implement it.
	}
}
