package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.clients.enhance.ExtMessage;

import java.util.List;

/**
 * ConcurrentMessageHandler
 */
public interface ConcurrentMessageHandler<K> extends MessageHandler<K, ConcurrentConsumeContext> {
    @Override
    ConsumeStatus consumeMessage(List<ExtMessage<K>> message, ConcurrentConsumeContext consumeContext) throws InterruptedException;
}
