package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.clients.enhance.ExtMessage;

import java.util.List;

/**
 * Created by steven03.zhang on 2018/1/22.
 */
public interface MessageHandler<K, C extends AbsConsumeHandlerContext> {
    ConsumeStatus consumeMessage(List<ExtMessage<K>> message, C consumeContext) throws InterruptedException;
}
