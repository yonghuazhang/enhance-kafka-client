package org.apache.kafka.clients.enhance.producer;

import org.apache.kafka.clients.enhance.ExtMessage;
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
        super(configs);
    }

    public KafkaProducerProxy(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<ExtMessage<K>> valueSerializer) {
        super(configs, keySerializer, valueSerializer);
    }

    public KafkaProducerProxy(Properties properties) {
        super(properties);
    }

    public KafkaProducerProxy(Properties properties, Serializer<K> keySerializer, Serializer<ExtMessage<K>> valueSerializer) {
        super(properties, keySerializer, valueSerializer);
    }

    void addProducerMessageHook(SendMessageHook<K> hook) {
        hookWriteLock.lock();
        try {
            if (null == this.interceptors) {
                SendMessageHooks<K> tmpInterceptors = new SendMessageHooks<>();
                tmpInterceptors.addSendMessageHook(hook);
                this.interceptors = tmpInterceptors;
            } else if (interceptors instanceof SendMessageHooks) {
                ((SendMessageHooks) this.interceptors).addSendMessageHook(hook);
            } else {
                SendMessageHooks<K> tmpInterceptors = new SendMessageHooks<>();
                tmpInterceptors.addSendMessageHook(this.interceptors.getInterceptors());
                this.interceptors = tmpInterceptors;
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
