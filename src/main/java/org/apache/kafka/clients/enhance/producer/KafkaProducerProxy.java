package org.apache.kafka.clients.enhance.producer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.ExtMessageEncoder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class KafkaProducerProxy<K> extends KafkaProducer<K, ExtMessage<K>> {
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerProxy.class);

	private final ReentrantReadWriteLock hookLock = new ReentrantReadWriteLock();
	private final ReentrantReadWriteLock.WriteLock hookWriteLock = hookLock.writeLock();
	private final ReentrantReadWriteLock.ReadLock hookReadLock = hookLock.readLock();

	public KafkaProducerProxy(Map<String, Object> configs) {
		this(configs, null);
	}

	public KafkaProducerProxy(Map<String, Object> configs, Serializer<K> keySerializer) {
		super(configs, keySerializer, new ExtMessageEncoder<K>());
	}

	public KafkaProducerProxy(Properties properties) {
		this(properties, null);
	}

	public KafkaProducerProxy(Properties properties, Serializer<K> keySerializer) {
		super(properties, keySerializer, new ExtMessageEncoder<K>());
	}

	void addProducerMessageHooks(SendMessageHooks<K> hooks) {
		hookWriteLock.lock();
		try {
			if (null == this.interceptors) {
				this.interceptors = hooks;
			} else if (interceptors instanceof SendMessageHooks) {
				((SendMessageHooks) this.interceptors).addSendMessageHook(hooks.getInterceptors());
			} else {
				hooks.addSendMessageHook(this.interceptors.getInterceptors());
				this.interceptors = hooks;
			}
		} finally {
			hookWriteLock.unlock();
		}

	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, ExtMessage<K>> record) {
		hookReadLock.lock();
		try {
			return super.send(record);
		} finally {
			hookReadLock.unlock();
		}
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, ExtMessage<K>> record, Callback callback) {
		hookReadLock.lock();
		try {
			return super.send(record, callback);
		} finally {
			hookReadLock.unlock();
		}
	}
}
