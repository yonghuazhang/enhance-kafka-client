package org.apache.kafka.clients.enhance.producer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class SendMessageHooks<K> extends ProducerInterceptors<K, ExtMessage<K>> {

    public SendMessageHooks() {
        super(new ArrayList<ProducerInterceptor<K, ExtMessage<K>>>());
    }

    public void addSendMessageHook(final SendMessageHook<K> hook) {
        this.interceptors.add(new ProducerInterceptor<K, ExtMessage<K>>() {
            @Override
            public ProducerRecord<K, ExtMessage<K>> onSend(ProducerRecord<K, ExtMessage<K>> record) {
                return new ProducerRecord<>(record.topic(), record.key(), hook.beforeSend(record.value()));
            }

            @Override
            public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
                hook.afterSend(metadata, exception);
            }

            @Override
            public void close() {

            }

            @Override
            public void configure(Map<String, ?> configs) {

            }
        });
    }

    public void addSendMessageHook(final ProducerInterceptor<K, ExtMessage<K>> interceptor) {
        this.interceptors.add(interceptor);
    }

    public void addSendMessageHook(final List<ProducerInterceptor<K, ExtMessage<K>>> interceptors) {
        this.interceptors.addAll(interceptors);
    }

    public void clearHooks() {
        this.interceptors.clear();
    }

    public boolean isEmpty() {
        return this.interceptors.isEmpty();
    }
}
