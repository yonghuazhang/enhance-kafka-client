package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConsumeMessageHooks<K> {
    private final static Logger logger = LoggerFactory.getLogger(ConsumeMessageHooks.class);
    private final List<ConsumeMessageHook<K>> hooks;

    public ConsumeMessageHooks() {
        this.hooks = Collections.synchronizedList(new ArrayList<ConsumeMessageHook<K>>());
    }

    public List<ExtMessage<K>> execConsumeBefore(ConsumeHookContext hookContext,
                                                 List<ExtMessage<K>> messages) {
        if (!this.hooks.isEmpty()) {

        }
        return null;
    }

    public List<ExtMessage<K>> execConsumeAfter(ConsumeHookContext hookContext,
                                                List<ExtMessage<K>> messages) {
        if (!this.hooks.isEmpty()) {

        }
        return null;
    }

    public void addMessageHook(ConsumeMessageHook<K> hook) {
        this.hooks.add(hook);
    }

    public void clearHooks() {
        this.hooks.clear();
    }

}
