package org.apache.kafka.clients.enhance;

import org.apache.kafka.common.header.Headers;

public abstract class AbsExtMessageFilter<K> {
    protected boolean permitAll = false;

    public abstract boolean canDeliveryMessage(ExtMessage<K> message, Headers headers);

    public boolean isPermitAll() {
        return permitAll;
    }
}
