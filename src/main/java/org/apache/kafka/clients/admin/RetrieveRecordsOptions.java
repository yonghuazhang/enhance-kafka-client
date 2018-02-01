package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Created by steven03.zhang on 2017/8/28.
 */
@InterfaceStability.Evolving
public class RetrieveRecordsOptions {
    private Integer timeoutMs = null;

    public RetrieveRecordsOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public Integer timeoutMs() {
        return timeoutMs;
    }
}
