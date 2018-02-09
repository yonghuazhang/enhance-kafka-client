package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.clients.enhance.ExtMessage;

import java.util.List;

/**
 * ConcurrentMessageHandler
 */
public interface ConcurrentMessageHandler<K> extends MessageHandler<K, ConcurrentConsumeHandlerContext> {
    @Override
    ConsumeStatus consumeMessage(List<ExtMessage<K>> message, ConcurrentConsumeHandlerContext consumeContext) throws InterruptedException;
}
