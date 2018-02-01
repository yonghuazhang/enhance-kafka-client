package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.clients.enhance.ExtMessage;

import java.util.List;

public interface ConsumeMessageHook<K> {

    List<ExtMessage<K>> execConsumeBefore(ConsumeHookContext hookContext, List<ExtMessage<K>> messages);

    List<ExtMessage<K>> execConsumeAfter(ConsumeHookContext hookContext, List<ExtMessage<K>> messages);
}
