package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.clients.enhance.ExtMessage;

import java.util.List;

public interface MessageHandler<K, C extends AbstractConsumeContext> {
    ConsumeStatus consumeMessage(List<ExtMessage<K>> message, C consumeContext) throws InterruptedException;
}
