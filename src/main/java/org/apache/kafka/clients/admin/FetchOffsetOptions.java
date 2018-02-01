package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Created by steven03.zhang on 2017/9/11.
 */
@InterfaceStability.Evolving
public class FetchOffsetOptions {
    private Integer timeoutMs = null;

    /**
     * Set the request timeout in milliseconds for this operation or {@code null} if the default request timeout for the
     * AdminClient should be used.
     */
    public FetchOffsetOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    /**
     * The request timeout in milliseconds for this operation or {@code null} if the default request timeout for the
     * AdminClient should be used.
     */
    public Integer timeoutMs() {
        return timeoutMs;
    }
}
