package org.apache.kafka.clients.enhance;

import org.apache.kafka.common.header.Headers;

public interface ExtMessageFilter<K> {
    boolean canDelieveryMessage(ExtMessage<K> message, Headers headers);
}
