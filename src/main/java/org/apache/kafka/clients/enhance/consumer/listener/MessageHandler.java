package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.clients.enhance.ExtMessage;

import java.util.List;

/**
 * Created by steven03.zhang on 2017/12/14.
 */
public interface MessageHandler<K, C extends AbsConsumeHandlerContext> {
    ConsumeStatus consumeMessage(List<ExtMessage<K>> message, C consumeContext) throws InterruptedException;
}
